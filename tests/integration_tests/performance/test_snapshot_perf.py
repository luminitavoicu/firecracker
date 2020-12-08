# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Basic tests scenarios for snapshot save/restore."""

import json
import logging
import os
import platform
import pytest
from conftest import _test_images_s3_bucket
from framework.artifacts import ArtifactCollection, ArtifactSet
from framework.matrix import TestMatrix, TestContext
from framework.microvms import VMMicro
from framework.builder import MicrovmBuilder, SnapshotBuilder, SnapshotType
from framework.statistics import core, consumer, producer, types, criteria, \
    function
from framework.utils import eager_map
import host_tools.network as net_tools  # pylint: disable=import-error
import host_tools.logging as log_tools

# Supported snapshot target versions for the current Firecracker version.
FIRECRACKER_VERSIONS = {
    '0.23.0',
    '0.24.0',
}

# How many latencies do we sample per test.
SAMPLE_COUNT = 3
USEC_IN_MSEC = 1000
LATENCY = 'latency'

# Latencies in milliseconds.
CREATE_LATENCY_BASELINES = {
    '2vcpu_256mb.json': {
        'FULL':  {
            "target": 250,
            "delta": 15
        },
        'DIFF':  {
            "target": 16,
            "delta": 1
        },
    },
    '2vcpu_512mb.json': {
        'FULL':  {
            "target": 250,
            "delta": 15
        },
        'DIFF':  {
            "target": 16,
            "delta": 1
        },
    }
}

# The latencies are pretty high during integration tests and
# this is tracked here:
# https://github.com/firecracker-microvm/firecracker/issues/2027
# TODO: Update the table after fix. Target is < 5ms.
LOAD_LATENCY_BASELINES = {
    '2vcpu_256mb.json': 8,
    '2vcpu_512mb.json': 8,
}

def pass_criteria(microvm_name, snapshot_type):
    """Define pass criteria for the statistics."""
    if snapshot_type == SnapshotType.FULL:
        type = "FULL"
    else:
        type = "DIFF"

    delta = CREATE_LATENCY_BASELINES[microvm_name][type]["delta"]
    target = CREATE_LATENCY_BASELINES[microvm_name][type]["target"]

    print("passing criteria: " + str(snapshot_type))
    return {
        types.DefaultStat.AVG.name: criteria.EqualWith(target, delta)
    }


def measurements():
    """Define the produced measurements."""
    return [types.MeasurementDef(LATENCY, "millisecond")]


def stats(microvm_name, snapshot_type):
    """Define statistics based on the measurements."""
    print("stats func")
    return types.StatisticDef.defaults(
        LATENCY,
        pass_criteria(microvm_name, snapshot_type)
    )


def create_snapshot(context, target_version):
    print("enter producer func")
    vm_builder = context.custom['builder']
    snapshot_type = context.custom['snapshot_type']
    enable_diff_snapshots = snapshot_type == SnapshotType.DIFF

    # Create a rw copy artifact.
    rw_disk = context.disk.copy()
    # Get ssh key from read-only artifact.
    ssh_key = context.disk.ssh_key()

    # Create a fresh microvm from aftifacts.
    vm = vm_builder.build(kernel=context.kernel,
                          disks=[rw_disk],
                          ssh_key=ssh_key,
                          config=context.microvm,
                          enable_diff_snapshots=enable_diff_snapshots)

    # Configure metrics system.
    metrics_fifo_path = os.path.join(vm.path, 'metrics_fifo')
    metrics_fifo = log_tools.Fifo(metrics_fifo_path)

    response = vm.metrics.put(
        metrics_path=vm.create_jailed_resource(metrics_fifo.path)
    )
    assert vm.api_session.is_status_no_content(response.status_code)

    vm.start()

    # Create a snapshot builder from a microvm.
    snapshot_builder = SnapshotBuilder(vm)
    snapshot_builder.create(disks=[rw_disk],
                            ssh_key=ssh_key,
                            snapshot_type=snapshot_type,
                            target_version=target_version)
    metrics = vm.flush_metrics(metrics_fifo)

    vm.kill()
    print("returning metrics")
    return metrics


def consume_create_latencies(cons, metrics, context):
    print("enter consumer func")
    snapshot_type = context.custom['snapshot_type']
    microvm_name = context.microvm.name()

    print("before setting defs")
    eager_map(cons.set_measurement_def, measurements())
    eager_map(cons.set_stat_def, stats(microvm_name, snapshot_type))
    print("after setting defs")

    if snapshot_type == SnapshotType.FULL:
        value = metrics['latencies_us']['full_create_snapshot'] / USEC_IN_MSEC
    else:
        value = metrics['latencies_us']['diff_create_snapshot'] / USEC_IN_MSEC

    cons.consume_stat(st_name=LATENCY,
                      ms_name=LATENCY,
                      value=value)


def _test_snapshot_create_latency(context):
    logger = context.custom['logger']
    snapshot_type = context.custom['snapshot_type']

    # Test snapshot creation for every supported target version.
    for target_version in FIRECRACKER_VERSIONS:
        # v0.23 does not support diff snapshots
        if "v0.23" in target_version and snapshot_type == SnapshotType.DIFF:
            continue

        logger.info("""Measuring snapshot create({}) latency for target
        version: {} and microvm: \"{}\", kernel {}, disk {} """
                    .format(snapshot_type,
                            target_version,
                            context.microvm.name(),
                            context.kernel.name(),
                            context.disk.name()))

        custom = {"microvm": context.microvm.name(),
                  "kernel": context.kernel.name(),
                  "disk": context.disk.name()}
        st_core = core.Core(name="snapshot_create_latency",
                            iterations=1,
                            custom=custom)

        # Configure a consumer that scans the latency metrics.
        cons = consumer.LambdaConsumer(
            consume_stats=True,
            func=consume_create_latencies,
            func_kwargs={"context": context}
        )
        # Configure a producer that creates a fresh microvm from artifacts
        # and takes a snapshot using the specified target version and type.
        prod = producer.LambdaProducer(
            func=create_snapshot,
            func_kwargs={"context": context, "target_version": target_version}
        )

        st_core.add_pipe(producer=prod, consumer=cons, tag="create_snapshot")

        # Gather results and verify pass criteria.
        st_core.run_exercise()

        assert 1 == 2


def _test_snapshot_resume_latency(context):
    logger = context.custom['logger']
    vm_builder = context.custom['builder']
    snapshot_type = context.custom['snapshot_type']
    enable_diff_snapshots = snapshot_type == SnapshotType.DIFF

    logger.info("""Measuring snapshot resume({}) latency for microvm: \"{}\",
kernel {}, disk {} """.format(snapshot_type,
                              context.microvm.name(),
                              context.kernel.name(),
                              context.disk.name()))

    # Create a rw copy artifact.
    rw_disk = context.disk.copy()
    # Get ssh key from read-only artifact.
    ssh_key = context.disk.ssh_key()
    # Create a fresh microvm from aftifacts.
    basevm = vm_builder.build(kernel=context.kernel,
                              disks=[rw_disk],
                              ssh_key=ssh_key,
                              config=context.microvm,
                              enable_diff_snapshots=enable_diff_snapshots)

    basevm.start()
    ssh_connection = net_tools.SSHConnection(basevm.ssh_config)

    # Check if guest works.
    exit_code, _, _ = ssh_connection.execute_command("sync")
    assert exit_code == 0

    logger.info("Create {}.".format(snapshot_type))
    # Create a snapshot builder from a microvm.
    snapshot_builder = SnapshotBuilder(basevm)

    snapshot = snapshot_builder.create([rw_disk.local_path()],
                                       ssh_key,
                                       snapshot_type)

    basevm.kill()

    for i in range(SAMPLE_COUNT):
        microvm, metrics_fifo = vm_builder.build_from_snapshot(
                                                snapshot,
                                                True,
                                                enable_diff_snapshots)

        # Attempt to connect to resumed microvm.
        ssh_connection = net_tools.SSHConnection(microvm.ssh_config)

        # Verify if guest can run commands.
        exit_code, _, _ = ssh_connection.execute_command("sync")
        assert exit_code == 0

        value = 0
        # Parse all metric data points in search of load_snapshot time.
        metrics = microvm.get_all_metrics(metrics_fifo)
        for data_point in metrics:
            metrics = json.loads(data_point)
            cur_value = metrics['latencies_us']['load_snapshot'] / USEC_IN_MSEC
            if cur_value > 0:
                value = cur_value
                break

        baseline = LOAD_LATENCY_BASELINES[context.microvm.name()]
        logger.info("Latency {}/{}: {} ms".format(i + 1, SAMPLE_COUNT, value))
        assert baseline > value, "LoadSnapshot latency degraded."

        microvm.kill()


@pytest.mark.skipif(
    platform.machine() != "x86_64",
    reason="Not supported yet."
)
def test_snapshot_create_full_latency(network_config,
                                      bin_cloner_path):
    """Test scenario: Full snapshot create performance measurement."""
    logger = logging.getLogger("snapshot_sequence")
    artifacts = ArtifactCollection(_test_images_s3_bucket())
    # Testing matrix:
    # - Guest kernel: Linux 4.14
    # - Rootfs: Ubuntu 18.04
    # - Microvm: 2vCPU with 256/512 MB RAM
    # TODO: Multiple microvm sizes must be tested in the async pipeline.
    microvm_artifacts = ArtifactSet(artifacts.microvms(keyword="2vcpu_512mb"))
    microvm_artifacts.insert(artifacts.microvms(keyword="2vcpu_256mb"))
    kernel_artifacts = ArtifactSet(artifacts.kernels(keyword="4.14"))
    disk_artifacts = ArtifactSet(artifacts.disks(keyword="ubuntu"))

    # Create a test context and add builder, logger, network.
    test_context = TestContext()
    test_context.custom = {
        'builder': MicrovmBuilder(bin_cloner_path),
        'network_config': network_config,
        'logger': logger,
        'snapshot_type': SnapshotType.FULL,
    }

    # Create the test matrix.
    test_matrix = TestMatrix(context=test_context,
                             artifact_sets=[
                                 microvm_artifacts,
                                 kernel_artifacts,
                                 disk_artifacts
                             ])

    test_matrix.run_test(_test_snapshot_create_latency)


def test_snapshot_create_diff_latency(network_config,
                                      bin_cloner_path):
    """Test scenario: Diff snapshot create performance measurement."""
    logger = logging.getLogger("snapshot_sequence")
    artifacts = ArtifactCollection(_test_images_s3_bucket())
    # Testing matrix:
    # - Guest kernel: Linux 4.14
    # - Rootfs: Ubuntu 18.04
    # - Microvm: 2vCPU with 256/512 MB RAM
    # TODO: Multiple microvm sizes must be tested in the async pipeline.
    microvm_artifacts = ArtifactSet(artifacts.microvms(keyword="2vcpu_512mb"))
    microvm_artifacts.insert(artifacts.microvms(keyword="2vcpu_256mb"))
    kernel_artifacts = ArtifactSet(artifacts.kernels(keyword="4.14"))
    disk_artifacts = ArtifactSet(artifacts.disks(keyword="ubuntu"))

    # Create a test context and add builder, logger, network.
    test_context = TestContext()
    test_context.custom = {
        'builder': MicrovmBuilder(bin_cloner_path),
        'network_config': network_config,
        'logger': logger,
        'snapshot_type': SnapshotType.DIFF,
    }

    # Create the test matrix.
    test_matrix = TestMatrix(context=test_context,
                             artifact_sets=[
                                 microvm_artifacts,
                                 kernel_artifacts,
                                 disk_artifacts
                             ])

    test_matrix.run_test(_test_snapshot_create_latency)


@pytest.mark.skipif(
    platform.machine() != "x86_64",
    reason="Not supported yet."
)
def test_snapshot_resume_latency(network_config,
                                 bin_cloner_path):
    """Test scenario: Snapshot load performance measurement."""
    logger = logging.getLogger("snapshot_load")

    artifacts = ArtifactCollection(_test_images_s3_bucket())
    # Testing matrix:
    # - Guest kernel: Linux 4.14
    # - Rootfs: Ubuntu 18.04
    # - Microvm: 2vCPU with 256/512 MB RAM
    # TODO: Multiple microvm sizes must be tested in the async pipeline.
    microvm_artifacts = ArtifactSet(artifacts.microvms(keyword="2vcpu_512mb"))
    microvm_artifacts.insert(artifacts.microvms(keyword="2vcpu_256mb"))

    kernel_artifacts = ArtifactSet(artifacts.kernels(keyword="4.14"))
    disk_artifacts = ArtifactSet(artifacts.disks(keyword="ubuntu"))

    # Create a test context and add builder, logger, network.
    test_context = TestContext()
    test_context.custom = {
        'builder': MicrovmBuilder(bin_cloner_path),
        'network_config': network_config,
        'logger': logger,
        'snapshot_type': SnapshotType.FULL,
    }

    # Create the test matrix.
    test_matrix = TestMatrix(context=test_context,
                             artifact_sets=[
                                 microvm_artifacts,
                                 kernel_artifacts,
                                 disk_artifacts
                             ])

    test_matrix.run_test(_test_snapshot_resume_latency)


@pytest.mark.skipif(
    platform.machine() != "x86_64",
    reason="Not supported yet."
)
def test_older_snapshot_resume_latency(bin_cloner_path):
    """Test scenario: Older snapshot load performance measurement."""
    logger = logging.getLogger("old_snapshot_load")

    artifacts = ArtifactCollection(_test_images_s3_bucket())
    # Fetch all firecracker binaries.
    # With each binary create a snapshot and try to restore in current
    # version.
    firecracker_artifacts = artifacts.firecrackers()
    for firecracker in firecracker_artifacts:
        firecracker.download()
        jailer = firecracker.jailer()
        jailer.download()
        fc_version = firecracker.base_name()[1:]
        logger.info("Firecracker version: %s", fc_version)
        logger.info("Source Firecracker: %s", firecracker.local_path())
        logger.info("Source Jailer: %s", jailer.local_path())

        for i in range(SAMPLE_COUNT):
            # Create a fresh microvm with the binary artifacts.
            vm_instance = VMMicro.spawn(bin_cloner_path, True,
                                        firecracker.local_path(),
                                        jailer.local_path())
            # Attempt to connect to the fresh microvm.
            ssh_connection = net_tools.SSHConnection(vm_instance.vm.ssh_config)

            exit_code, _, _ = ssh_connection.execute_command("sync")
            assert exit_code == 0

            # The snapshot builder expects disks as paths, not artifacts.
            disks = []
            for disk in vm_instance.disks:
                disks.append(disk.local_path())

            # Create a snapshot builder from a microvm.
            snapshot_builder = SnapshotBuilder(vm_instance.vm)
            snapshot = snapshot_builder.create(disks,
                                               vm_instance.ssh_key,
                                               SnapshotType.FULL)

            vm_instance.vm.kill()
            builder = MicrovmBuilder(bin_cloner_path)
            microvm, metrics_fifo = builder.build_from_snapshot(snapshot,
                                                                True,
                                                                False)
            # Attempt to connect to resumed microvm.
            ssh_connection = net_tools.SSHConnection(microvm.ssh_config)
            # Check if guest still runs commands.
            exit_code, _, _ = ssh_connection.execute_command("dmesg")
            assert exit_code == 0

            value = 0
            # Parse all metric data points in search of load_snapshot time.
            metrics = microvm.get_all_metrics(metrics_fifo)
            for data_point in metrics:
                metrics = json.loads(data_point)
                cur_value = metrics['latencies_us']['load_snapshot']
                if cur_value > 0:
                    value = cur_value / USEC_IN_MSEC
                    break

            baseline = LOAD_LATENCY_BASELINES['2vcpu_512mb.json']
            logger.info("Latency %s/%s: %s ms", i + 1, SAMPLE_COUNT, value)
            assert baseline > value, "LoadSnapshot latency degraded."
            microvm.kill()
