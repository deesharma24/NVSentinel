# Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import os
import tempfile
import time
import unittest
from concurrent import futures
from threading import Event
from typing import Any

import grpc
from gpu_health_monitor.platform_connector import platform_connector

from gpu_health_monitor.protos import (
    health_event_pb2 as platformconnector_pb2,
    health_event_pb2_grpc as platformconnector_pb2_grpc,
)

socket_path = "/tmp/nvsentinel.sock"
node_name = "test-node-1"


def sample_metadata():
    """Sample GPU metadata with 4 GPUs for testing."""
    return {
        "version": "1.0",
        "timestamp": "2025-11-07T10:00:00Z",
        "node_name": "test-node",
        "chassis_serial": "CHASSIS-12345",
        "gpus": [
            {
                "gpu_id": 0,
                "uuid": "GPU-00000000-0000-0000-0000-000000000000",
                "pci_address": "0000:04:00.0",
                "serial_number": "SN-GPU-0",
                "device_name": "NVIDIA H100",
                "nvlinks": [],
            },
            {
                "gpu_id": 1,
                "uuid": "GPU-11111111-1111-1111-1111-111111111111",
                "pci_address": "0000:05:00.0",
                "serial_number": "SN-GPU-1",
                "device_name": "NVIDIA H100",
                "nvlinks": [],
            },
            {
                "gpu_id": 2,
                "uuid": "GPU-22222222-2222-2222-2222-222222222222",
                "pci_address": "0000:41:00.0",
                "serial_number": "SN-GPU-2",
                "device_name": "NVIDIA H100",
                "nvlinks": [],
            },
            {
                "gpu_id": 3,
                "uuid": "GPU-33333333-3333-3333-3333-333333333333",
                "pci_address": "0000:82:00.0",
                "serial_number": "SN-GPU-3",
                "device_name": "NVIDIA H100",
                "nvlinks": [],
            },
        ],
    }


def metadata_file():
    """Create a temporary metadata file for testing."""
    f = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json")
    json.dump(sample_metadata(), f)
    return f.name


class PlatformConnectorServicer(platformconnector_pb2_grpc.PlatformConnectorServicer):
    def __init__(self) -> None:
        self.health_events: list[platformconnector_pb2.HealthEvent] = []

    def HealthEventOccurredV1(self, request: platformconnector_pb2.HealthEvents, context: Any):
        self.health_events = list(request.events)
        return platformconnector_pb2.HealthEvents()


class TestGpuCountMismatch(unittest.TestCase):
    def setUp(self):
        self.healthEventProcessor = PlatformConnectorServicer()
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        platformconnector_pb2_grpc.add_PlatformConnectorServicer_to_server(self.healthEventProcessor, self.server)
        self.server.add_insecure_port(f"unix://{socket_path}")
        self.server.start()

        self.exit = Event()
        self.temp_file_path = metadata_file()

        dcgm_health_conditions_categorization_mapping_config = {
            "DCGM_HEALTH_WATCH_PCIE": "Fatal",
            "DCGM_HEALTH_WATCH_NVLINK": "Fatal",
        }

        self.processor = platform_connector.PlatformConnectorEventProcessor(
            socket_path=socket_path,
            node_name=node_name,
            exit=self.exit,
            dcgm_errors_info_dict={},
            state_file_path="statefile",
            dcgm_health_conditions_categorization_mapping_config=dcgm_health_conditions_categorization_mapping_config,
            metadata_path=self.temp_file_path,
            processing_strategy=platformconnector_pb2.EXECUTE_REMEDIATION,
        )

    def tearDown(self):
        self.server.stop(0)
        if os.path.exists(self.temp_file_path):
            os.unlink(self.temp_file_path)
        # Clean up the UDS socket file if it exists
        if os.path.exists(socket_path):
            os.unlink(socket_path)

    def test_mismatch_sends_unhealthy_event(self):
        """Test that a GPU count mismatch fires an unhealthy health event with correct fields."""
        # sysfs sees 4 GPUs, DCGM sees 3 (GPU 3 missing from driver)
        sysfs_pci_addresses = ["0000:04:00.0", "0000:05:00.0", "0000:41:00.0", "0000:82:00.0"]
        dcgm_gpu_ids = [0, 1, 2]

        self.processor.gpu_count_check_completed(sysfs_pci_addresses, dcgm_gpu_ids)

        health_events = self.healthEventProcessor.health_events
        assert len(health_events) == 1

        event = health_events[0]
        assert event.checkName == "GpuTotalCountMismatch"
        assert event.isFatal is True
        assert event.isHealthy is False
        assert event.errorCode == ["GPU_TOTAL_COUNT_MISMATCH"]
        assert event.recommendedAction == platformconnector_pb2.RESTART_VM
        assert event.nodeName == node_name
        assert event.agent == "gpu-health-monitor"
        assert event.componentClass == "GPU"
        assert event.processingStrategy == platformconnector_pb2.EXECUTE_REMEDIATION

        # The missing GPU (0000:82:00.0) should be in entities impacted
        assert len(event.entitiesImpacted) == 1
        assert event.entitiesImpacted[0].entityType == "PCI"
        assert event.entitiesImpacted[0].entityValue == "0000:82:00.0"

        # Message should include counts and missing addresses
        assert "4 GPUs found on PCIe bus" in event.message
        assert "3 visible to S/W" in event.message
        assert "0000:82:00.0" in event.message
        assert "Run nvidia-smi" in event.message

        # Metadata should include chassis serial
        assert event.metadata["chassis_serial"] == "CHASSIS-12345"

    def test_match_sends_healthy_event_on_first_cycle(self):
        """Test that matching GPU counts send a healthy event on first cycle (cache empty)."""
        sysfs_pci_addresses = ["0000:04:00.0", "0000:05:00.0", "0000:41:00.0", "0000:82:00.0"]
        dcgm_gpu_ids = [0, 1, 2, 3]

        self.processor.gpu_count_check_completed(sysfs_pci_addresses, dcgm_gpu_ids)

        health_events = self.healthEventProcessor.health_events
        assert len(health_events) == 1

        event = health_events[0]
        assert event.checkName == "GpuTotalCountMismatch"
        assert event.isHealthy is True
        assert event.isFatal is False
        assert event.recommendedAction == platformconnector_pb2.NONE
        assert "reported no errors" in event.message

        # Verify cache is populated
        key = self.processor._build_cache_key("GpuTotalCountMismatch", "GPU_TOTAL_COUNT", "ALL")
        assert key in self.processor.entity_cache
        assert self.processor.entity_cache[key].isHealthy is True

    def test_match_no_event_on_second_cycle(self):
        """Test that matching GPU counts do not send a duplicate event on subsequent cycles."""
        sysfs_pci_addresses = ["0000:04:00.0", "0000:05:00.0", "0000:41:00.0", "0000:82:00.0"]
        dcgm_gpu_ids = [0, 1, 2, 3]

        # First cycle: sends healthy event
        self.processor.gpu_count_check_completed(sysfs_pci_addresses, dcgm_gpu_ids)
        assert len(self.healthEventProcessor.health_events) == 1

        # Reset the servicer to track new events
        self.healthEventProcessor.health_events = []

        # Second cycle: cache already has healthy state, no event sent
        self.processor.gpu_count_check_completed(sysfs_pci_addresses, dcgm_gpu_ids)
        assert len(self.healthEventProcessor.health_events) == 0

    def test_entity_cache_prevents_duplicate_sends(self):
        """Test that the entity cache prevents sending duplicate mismatch events."""
        sysfs_pci_addresses = ["0000:04:00.0", "0000:05:00.0", "0000:41:00.0", "0000:82:00.0"]
        dcgm_gpu_ids = [0, 1, 2]

        # First call should send the event
        self.processor.gpu_count_check_completed(sysfs_pci_addresses, dcgm_gpu_ids)
        assert len(self.healthEventProcessor.health_events) == 1

        # Reset the servicer to track new events
        self.healthEventProcessor.health_events = []

        # Second call should be suppressed by entity cache
        self.processor.gpu_count_check_completed(sysfs_pci_addresses, dcgm_gpu_ids)
        assert len(self.healthEventProcessor.health_events) == 0

    def test_mismatch_then_resolved(self):
        """Test full lifecycle: mismatch detected then resolved."""
        sysfs_pci_addresses = ["0000:04:00.0", "0000:05:00.0", "0000:41:00.0", "0000:82:00.0"]
        dcgm_gpu_ids_missing = [0, 1, 2]
        dcgm_gpu_ids_full = [0, 1, 2, 3]

        # First: mismatch (sysfs=4, DCGM=3)
        self.processor.gpu_count_check_completed(sysfs_pci_addresses, dcgm_gpu_ids_missing)
        events = self.healthEventProcessor.health_events
        assert len(events) == 1
        assert events[0].isHealthy is False
        assert events[0].checkName == "GpuTotalCountMismatch"

        # Verify cache has the mismatch state
        key = self.processor._build_cache_key("GpuTotalCountMismatch", "GPU_TOTAL_COUNT", "ALL")
        assert key in self.processor.entity_cache
        assert self.processor.entity_cache[key].isHealthy is False

        # Then: resolved (sysfs=4, DCGM=4)
        self.healthEventProcessor.health_events = []
        self.processor.gpu_count_check_completed(sysfs_pci_addresses, dcgm_gpu_ids_full)
        events = self.healthEventProcessor.health_events
        assert len(events) == 1
        resolved_event = events[0]
        assert resolved_event.isHealthy is True
        assert resolved_event.isFatal is False
        assert resolved_event.checkName == "GpuTotalCountMismatch"
        assert resolved_event.recommendedAction == platformconnector_pb2.NONE
        assert "reported no errors" in resolved_event.message

        # Verify cache is updated
        assert self.processor.entity_cache[key].isHealthy is True

    def test_multiple_missing_gpus(self):
        """Test mismatch with multiple GPUs missing from DCGM."""
        # sysfs sees 4 GPUs, DCGM sees 2 (GPU 2 and 3 missing)
        sysfs_pci_addresses = ["0000:04:00.0", "0000:05:00.0", "0000:41:00.0", "0000:82:00.0"]
        dcgm_gpu_ids = [0, 1]

        self.processor.gpu_count_check_completed(sysfs_pci_addresses, dcgm_gpu_ids)

        health_events = self.healthEventProcessor.health_events
        assert len(health_events) == 1

        event = health_events[0]
        # Both missing GPUs should be in entities impacted
        assert len(event.entitiesImpacted) == 2
        pci_values = [e.entityValue for e in event.entitiesImpacted]
        assert "0000:41:00.0" in pci_values
        assert "0000:82:00.0" in pci_values

        assert "4 GPUs found on PCIe bus" in event.message
        assert "2 visible to S/W" in event.message
