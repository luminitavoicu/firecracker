use std::fmt::{Display, Formatter};
use vm_memory::Bytes;
use vmm_sys_util::errno::Error;

pub use super::arch::x86_64::regs::setup_sregs;
use super::kvm_bindings::*;
use super::target::FirecrackerGDBServer;
use super::{Elf64_Phdr, GuestAddress, GuestMemoryMmap, VcpuFd, PT_LOAD};

const CR0_PG: u64 = 0x80000000;
const CR4_PAE: u64 = 0x00000020;
const CR4_LA57: u64 = 0x00001000;
const EFER_LME: u64 = 0x00000100;
const PDPTE_PS: u64 = 0x00000080;
const PDE_PS: u64 = 0x00000080;

const BIT_P: u64 = 0x1;

// [PML4E, PDPTE_PS0, PDPTE_PS1, PDE_PS0, PDE_PS1, PTE]
const TABLE_ENTRY_RSVD_BITS: [u64; 6] = [0x80, 0x0, 0x3fffe000, 0x0, 0x1fe000, 0x0];

const TABLE_ENTRY_MASK: u64 = 0x000ffffffffff000u64;

#[derive(PartialEq, Eq, Debug)]
enum PagingType {
    NONE,
    _32BIT,
    PAE,
    _4LVL,
    _5LVL,
}

pub enum DebugEvent {
    START,
    NOTIFY(FullVcpuState),
    GET_REGS,
    PEEK_REGS(FullVcpuState),
    CONTINUE(bool),
    STEP_INTO(bool),
}

#[derive(Default)]
pub struct FullVcpuState {
    pub regular_regs: kvm_regs,
    pub special_regs: kvm_sregs,
}

#[derive(Debug)]
pub enum DebuggerError {
    InvalidState,
    ChannelError,
    MemoryError,
    IoctlError(Error),
    InvalidLinearAddress,
    UnsupportedPagingStrategy,
}

impl Display for DebuggerError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            DebuggerError::InvalidState => write!(f, "[GDB Server] An invalid state was reached"),
            DebuggerError::ChannelError => write!(
                f,
                "[GDB Server] An error interrupted the GDB server - main thread communication"
            ),
            DebuggerError::MemoryError => write!(f, "[GDB Server] Failed access to guest memory"),
            DebuggerError::IoctlError(errno) => {
                write!(f, "[GDB Server] Failed ioctl call: {}", errno)
            }
            DebuggerError::InvalidLinearAddress => write!(
                f,
                "[GDB Server] An invalid linear address was passed from client"
            ),
            DebuggerError::UnsupportedPagingStrategy => write!(
                f,
                "A paging strategy that is not currently supported has been detected"
            ),
        }
    }
}

pub struct Debugger;

impl Debugger {
    pub fn enable_kvm_debug(vcpu: &VcpuFd, step: bool) -> Result<(), DebuggerError> {
        let mut control: __u32 = 0;
        control |= KVM_GUESTDBG_ENABLE | KVM_GUESTDBG_USE_SW_BP;
        if step {
            control |= KVM_GUESTDBG_SINGLESTEP;
        }
        let debug_struct = kvm_guest_debug {
            control: control,
            pad: 0,
            arch: kvm_guest_debug_arch {
                debugreg: [0, 0, 0, 0, 0, 0, 0, 0],
            },
        };

        if let Err(errno) = vcpu.set_guest_debug(&debug_struct) {
            return Err(DebuggerError::IoctlError(errno));
        }

        Ok(())
    }

    /// Function that performs guest page-walking, on top of the 4-Level paging mechanism,
    /// obtaining the physical address corresponding to the one received from a GDB client
    pub fn virt_to_phys(
        addr: u64,
        gm: &GuestMemoryMmap,
        guest_state: &FullVcpuState,
        e_phdrs: &Vec<Elf64_Phdr>,
    ) -> Result<u64, DebuggerError> {
        let mut linear_addr = addr;
        let context: kvm_sregs;
        let mut pt_level = PagingType::NONE;

        context = guest_state.special_regs;
        // Paging enabled
        if context.cr0 & CR0_PG != 0 {
            // Determine the type of paging
            // See Table 4.1, Volume 3A in Intel Arch SW Developer's Manual
            pt_level = if context.cr4 & CR4_LA57 != 0 {
                PagingType::_5LVL
            } else {
                if context.efer & EFER_LME != 0 {
                    PagingType::_4LVL
                } else {
                    if context.cr4 & CR4_PAE != 0 {
                        PagingType::PAE
                    } else {
                        PagingType::_32BIT
                    }
                }
            }
        }

        let mut paddr: u64 = 0;
        let mut mask: u64 = 0;
        let mut movem = 0;
        if pt_level == PagingType::_4LVL {
            // Translation from 48 bits linear address
            // to 52 bits physical address
            linear_addr &= 0x0000ffffffffffffu64;
            mask = 0x0000ff8000000000u64;

            paddr = context.cr3 & 0x000ffffffffff000u64;
            movem = 36;
        } else {
            return Err(DebuggerError::UnsupportedPagingStrategy);
        }

        paddr += (linear_addr & mask) >> movem;

        let mut table_entry;
        if let Ok(e) = gm.read_obj(GuestAddress(paddr)) {
            table_entry = e;
        } else {
            return Err(DebuggerError::MemoryError);
        }

        if Debugger::check_entry(table_entry, TABLE_ENTRY_RSVD_BITS[0]).is_err() {
            return Debugger::fixup_pointer(addr, e_phdrs);
        }

        // There is one loop iteration for each level (PDPT, PDT, PT);
        // However, the way we check for the validity of the entry
        // changes for the first two, depending on the PS flag. Therefore,
        // we have to either keep track of the index in the TABLE_ENTRY_RSVD_BITS
        // array or create individual const symbols for each possible value or
        // const symbols for each index
        let mut rsvd_idx = 0;
        for i in 0..3 {
            rsvd_idx = 2 * i + 1;

            mask >>= 9;
            movem -= 9;
            paddr = table_entry & TABLE_ENTRY_MASK;
            paddr += (linear_addr & mask) >> movem;
            if let Ok(e) = gm.read_obj(GuestAddress(paddr)) {
                table_entry = e;
            } else {
                return Err(DebuggerError::MemoryError);
            }

            match i {
                // translation to 1GB page
                0 => {
                    if (table_entry & PDPTE_PS) != 0 {
                        // Final address
                        paddr = table_entry & 0x000fffffc0000000u64;
                        paddr += linear_addr & 0x3fffffffu64;
                        rsvd_idx = 2 * i + 2;
                        break;
                    }
                }
                1 => {
                    if (table_entry & PDE_PS) != 0 {
                        // Final address
                        paddr = table_entry & 0x000fffffffe00000u64;
                        paddr += linear_addr & 0x1fffff;
                        rsvd_idx = 2 * i + 2;
                        break;
                    }
                }
                2 => {
                    // Final address
                    paddr = table_entry & 0x000ffffffffff000u64;
                    paddr += linear_addr & 0xfff;
                    break;
                }
                _ => {
                    return Err(DebuggerError::InvalidState);
                }
            }
            if Debugger::check_entry(table_entry, TABLE_ENTRY_RSVD_BITS[rsvd_idx]).is_err() {
                return Debugger::fixup_pointer(addr, e_phdrs);
            }
        }
        if Debugger::check_entry(table_entry, TABLE_ENTRY_RSVD_BITS[rsvd_idx]).is_err() {
            return Debugger::fixup_pointer(addr, e_phdrs);
        }

        Ok(paddr)
    }

    /// Checks whether the current table entry is valid (there is a valid translation
    /// for the linear address)
    fn check_entry(entry: u64, reserved_bits: u64) -> Result<(), DebuggerError> {
        if entry & BIT_P == 0 {
            return Err(DebuggerError::InvalidLinearAddress);
        }
        if entry & reserved_bits != 0 {
            return Err(DebuggerError::InvalidLinearAddress);
        }

        Ok(())
    }

    /// Following the kernel strategy, during the early boot phase, before the notion
    /// of virtual addresses has been put in place, we obtain the corresponding
    /// physical address by subtracting the section offset.
    fn fixup_pointer(addr: u64, e_phdrs: &Vec<Elf64_Phdr>) -> Result<u64, DebuggerError> {
        for phdr in e_phdrs {
            if (phdr.p_type & PT_LOAD) == 0 {
                continue;
            }
            if (phdr.p_vaddr <= addr) && (phdr.p_vaddr + phdr.p_memsz > addr) {
                return Ok(addr - phdr.p_vaddr + phdr.p_paddr);
            }
        }

        Err(DebuggerError::InvalidLinearAddress)
    }
}

#[cfg(test)]
mod tests {
    use super::setup_sregs;
    use kvm_ioctls::Kvm;
    use vm_memory::{GuestAddress, GuestMemoryMmap};

    #[test]
    fn test_virt_to_phys() {
        let kvm = Kvm::new().unwrap();
        let vm = kvm.create_vm().unwrap();
        let vcpu_fd = vm.create_vcpu(0).unwrap();
        let gm = GuestMemoryMmap::from_ranges(&[(GuestAddress(0), 0x10000)]).unwrap();

        setup_sregs(&gm, &vcpu_fd).unwrap();
        let state = super::FullVcpuState {
            regular_regs: vcpu_fd.get_regs().unwrap(),
            special_regs: vcpu_fd.get_sregs().unwrap(),
        };
        let e_phdrs = vec![
            super::Elf64_Phdr {
                p_type: super::PT_LOAD,
                p_flags: 0,
                p_offset: 0,
                p_vaddr: 0xffffffff81000000,
                p_paddr: 0x1000000,
                p_filesz: 11984896,
                p_memsz: 11984896,
                p_align: 0,
            },
            super::Elf64_Phdr {
                p_type: super::PT_LOAD,
                p_flags: 0,
                p_offset: 0,
                p_vaddr: 0xffffffff81cca000,
                p_paddr: 0x1cca000,
                p_filesz: 4243456,
                p_memsz: 4243456,
                p_align: 0,
            },
        ];
        // Testing translation through identity-mapped page tables set by Firecracker.
        // These page tables cover the first 1GB of memory, therefore we can only go
        // as far as 0x3ffffff
        assert_eq!(
            super::Debugger::virt_to_phys(0x1000000, &gm, &state, &e_phdrs).unwrap(),
            0x1000000
        );
        assert_eq!(
            super::Debugger::virt_to_phys(0xf000000, &gm, &state, &e_phdrs).unwrap(),
            0xf000000
        );
        assert_eq!(
            super::Debugger::virt_to_phys(0x3f000000, &gm, &state, &e_phdrs).unwrap(),
            0x3f000000
        );
        assert_eq!(
            super::Debugger::virt_to_phys(0x3fffffff, &gm, &state, &e_phdrs).unwrap(),
            0x3fffffff
        );
        // Testing translation thorugh elf binary information
        assert_eq!(
            super::Debugger::virt_to_phys(0xffffffff81ccab6b, &gm, &state, &e_phdrs).unwrap(),
            0x1ccab6b
        );
        assert_eq!(
            super::Debugger::virt_to_phys(0xffffffff81cde9b1, &gm, &state, &e_phdrs).unwrap(),
            0x1cde9b1
        );
        assert_eq!(
            super::Debugger::virt_to_phys(0xffffffff81365070, &gm, &state, &e_phdrs).unwrap(),
            0x1365070
        );
    }
}
