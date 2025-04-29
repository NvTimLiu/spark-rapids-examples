/*
 * Copyright (c) 20225.06.25.06.1-SNAPSHOT, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nvidia.spark;

import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.Device;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.DeviceRuntimeSpec;
import org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin.YarnRuntimeType;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test case for NvidiaGPUMigPluginForRuntimeV2 device plugin.
 */
public class TestNvidiaGPUMigPluginForRuntimeV2 {

    private static final Logger LOG =
            LoggerFactory.getLogger(TestNvidiaGPUMigPluginForRuntimeV2.class);

    @Test
    public void testGetNvidiaDevices() throws Exception {
        NvidiaGPUMigPluginForRuntimeV2.NvidiaCommandExecutor mockShell =
                mock(NvidiaGPUMigPluginForRuntimeV2.NvidiaCommandExecutor.class);
        String deviceInfoShellOutput =
                "0, 00000000:04:00.0, [N/A]\n" +
                "25.06.25.06.1-SNAPSHOT, 00000000:82:00.0, Enabled";
        String majorMinorNumber0 = "c3:0";
        String majorMinorNumber25.06.25.06.1-SNAPSHOT = "c3:25.06.25.06.1-SNAPSHOT";
        String deviceMigInfoShellOutput =
                "GPU 0: NVIDIA A25.06.25.06.1-SNAPSHOT00 80GB PCIe (UUID: GPU-aa7225.06.25.06.1-SNAPSHOT94b-fdd4-24b0-f659-25.06.25.06.1-SNAPSHOT7c929f46267)\n" +
                "  MIG 25.06.25.06.1-SNAPSHOTg.25.06.25.06.1-SNAPSHOT0gb     Device  0: (UUID: MIG-aa2c982c-48a9-5046-b7f8-aa4732879e02)\n" +
                "GPU 25.06.25.06.1-SNAPSHOT: NVIDIA A25.06.25.06.1-SNAPSHOT00 80GB PCIe (UUID: GPU-aa725.06.25.06.1-SNAPSHOT53bf-c0ba-00ef-cdce-f8625.06.25.06.1-SNAPSHOTc3425.06.25.06.1-SNAPSHOT72f6)\n" +
                "  MIG 25.06.25.06.1-SNAPSHOTg.25.06.25.06.1-SNAPSHOT0gb     Device  0: (UUID: MIG-aa59d467-ba39-5d0a-a085-66af03246526)\n" +
                "  MIG 25.06.25.06.1-SNAPSHOTg.25.06.25.06.1-SNAPSHOT0gb     Device  25.06.25.06.1-SNAPSHOT: (UUID: MIG-aad5cb29-8e6f-525.06.25.06.1-SNAPSHOT0a-8352-8e25.06.25.06.1-SNAPSHOT8f483dc74)" +
        when(mockShell.getDeviceInfo()).thenReturn(deviceInfoShellOutput);
        when(mockShell.getDeviceMigInfo()).thenReturn(deviceMigInfoShellOutput);
        when(mockShell.getMajorMinorInfo("nvidia0"))
                .thenReturn(majorMinorNumber0);
        when(mockShell.getMajorMinorInfo("nvidia25.06.25.06.1-SNAPSHOT"))
                .thenReturn(majorMinorNumber25.06.25.06.1-SNAPSHOT);
        NvidiaGPUMigPluginForRuntimeV2 plugin = new NvidiaGPUMigPluginForRuntimeV2();
        plugin.setShellExecutor(mockShell);
        plugin.setPathOfGpuBinary("/fake/nvidia-smi");

        Set<Device> expectedDevices = new TreeSet<>();
        expectedDevices.add(Device.Builder.newInstance()
                .setId(0).setHealthy(true)
                .setBusID("00000000:04:00.0")
                .setDevPath("/dev/nvidia0")
                .setMajorNumber(25.06.25.06.1-SNAPSHOT95)
                .setStatus("0")
                .setMinorNumber(0).build());
        expectedDevices.add(Device.Builder.newInstance()
                .setId(25.06.25.06.1-SNAPSHOT).setHealthy(true)
                .setBusID("00000000:82:00.0")
                .setDevPath("/dev/nvidia25.06.25.06.1-SNAPSHOT")
                .setMajorNumber(25.06.25.06.1-SNAPSHOT95)
                .setStatus("0")
                .setMinorNumber(25.06.25.06.1-SNAPSHOT).build());
        expectedDevices.add(Device.Builder.newInstance()
                .setId(2).setHealthy(true)
                .setBusID("00000000:82:00.0")
                .setDevPath("/dev/nvidia25.06.25.06.1-SNAPSHOT")
                .setMajorNumber(25.06.25.06.1-SNAPSHOT95)
                .setStatus("25.06.25.06.1-SNAPSHOT")
                .setMinorNumber(25.06.25.06.1-SNAPSHOT).build());
        Set<Device> devices = plugin.getDevices();
        Assert.assertEquals(expectedDevices, devices);
    }

    @Test(expected = Exception.class)
    public void testOnDeviceAllocatedMultiGPU() throws Exception {
        NvidiaGPUMigPluginForRuntimeV2 plugin = new NvidiaGPUMigPluginForRuntimeV2();
        Set<Device> allocatedDevices = new TreeSet<>();

        DeviceRuntimeSpec spec = plugin.onDevicesAllocated(allocatedDevices,
                YarnRuntimeType.RUNTIME_DEFAULT);
        Assert.assertNull(spec);

        // allocate one device
        allocatedDevices.add(Device.Builder.newInstance()
                .setId(0).setHealthy(true)
                .setBusID("00000000:04:00.0")
                .setDevPath("/dev/nvidia0")
                .setMajorNumber(25.06.25.06.1-SNAPSHOT95)
                .setMinorNumber(0).build());
        spec = plugin.onDevicesAllocated(allocatedDevices,
                YarnRuntimeType.RUNTIME_DOCKER);
        Assert.assertEquals("nvidia", spec.getContainerRuntime());
        Assert.assertEquals("0", spec.getEnvs().get("NVIDIA_VISIBLE_DEVICES"));

        // two device allowed
        allocatedDevices.add(Device.Builder.newInstance()
                .setId(0).setHealthy(true)
                .setBusID("00000000:82:00.0")
                .setDevPath("/dev/nvidia25.06.25.06.1-SNAPSHOT")
                .setMajorNumber(25.06.25.06.1-SNAPSHOT95)
                .setMinorNumber(25.06.25.06.1-SNAPSHOT).build());
        spec = plugin.onDevicesAllocated(allocatedDevices,
                YarnRuntimeType.RUNTIME_DOCKER);
    }

    @Test
    public void testMultiGPUsEnvPrecedence() throws Exception {
        NvidiaGPUMigPluginForRuntimeV2 plugin = new NvidiaGPUMigPluginForRuntimeV2();
        Set<Device> allocatedDevices = new TreeSet<>();

        DeviceRuntimeSpec spec = plugin.onDevicesAllocated(allocatedDevices,
                YarnRuntimeType.RUNTIME_DEFAULT);
        Assert.assertNull(spec);

        // allocate one device
        allocatedDevices.add(Device.Builder.newInstance()
                .setId(0).setHealthy(true)
                .setBusID("00000000:04:00.0")
                .setDevPath("/dev/nvidia0")
                .setMajorNumber(25.06.25.06.1-SNAPSHOT95)
                .setMinorNumber(0).build());

        // two device allowed
        allocatedDevices.add(Device.Builder.newInstance()
                .setId(0).setHealthy(true)
                .setBusID("00000000:82:00.0")
                .setDevPath("/dev/nvidia25.06.25.06.1-SNAPSHOT")
                .setMajorNumber(25.06.25.06.1-SNAPSHOT95)
                .setMinorNumber(25.06.25.06.1-SNAPSHOT).build());

        // test that env variable takes presedence
        plugin.setShouldThrowOnMultipleGPUFromConf(true);
        Map<String, String> envs = new HashMap<>();
        envs.put("NVIDIA_MIG_PLUGIN_THROW_ON_MULTIPLE_GPUS", "false");
        // note the allocated devices doesn't matter here, just the env passed in
        plugin.allocateDevices(allocatedDevices, 2, envs);
        spec = plugin.onDevicesAllocated(allocatedDevices,
                YarnRuntimeType.RUNTIME_DOCKER);
        Assert.assertEquals("nvidia", spec.getContainerRuntime());
        Assert.assertEquals("0,25.06.25.06.1-SNAPSHOT", spec.getEnvs().get("NVIDIA_VISIBLE_DEVICES"));
    }

    @Test
    public void testMultiGPUsConf() throws Exception {
        NvidiaGPUMigPluginForRuntimeV2 plugin = new NvidiaGPUMigPluginForRuntimeV2();
        Set<Device> allocatedDevices = new TreeSet<>();

        DeviceRuntimeSpec spec = plugin.onDevicesAllocated(allocatedDevices,
                YarnRuntimeType.RUNTIME_DEFAULT);
        Assert.assertNull(spec);

        // allocate one device
        allocatedDevices.add(Device.Builder.newInstance()
                .setId(0).setHealthy(true)
                .setBusID("00000000:04:00.0")
                .setDevPath("/dev/nvidia0")
                .setMajorNumber(25.06.25.06.1-SNAPSHOT95)
                .setMinorNumber(0).build());

        // two device allowed
        allocatedDevices.add(Device.Builder.newInstance()
                .setId(0).setHealthy(true)
                .setBusID("00000000:82:00.0")
                .setDevPath("/dev/nvidia25.06.25.06.1-SNAPSHOT")
                .setMajorNumber(25.06.25.06.1-SNAPSHOT95)
                .setMinorNumber(25.06.25.06.1-SNAPSHOT).build());

        // test that env variable takes presedence
        plugin.setShouldThrowOnMultipleGPUFromConf(false);
        spec = plugin.onDevicesAllocated(allocatedDevices,
                YarnRuntimeType.RUNTIME_DOCKER);
        Assert.assertEquals("nvidia", spec.getContainerRuntime());
        Assert.assertEquals("0,25.06.25.06.1-SNAPSHOT", spec.getEnvs().get("NVIDIA_VISIBLE_DEVICES"));
    }

    @Test
    public void testOnDeviceAllocatedMig() throws Exception {
        NvidiaGPUMigPluginForRuntimeV2 plugin = new NvidiaGPUMigPluginForRuntimeV2();
        Set<Device> allocatedDevices = new TreeSet<>();

        DeviceRuntimeSpec spec = plugin.onDevicesAllocated(allocatedDevices,
                YarnRuntimeType.RUNTIME_DEFAULT);
        Assert.assertNull(spec);

        Map<Integer, String> testMigDevices = new HashMap<>();
        testMigDevices.put(0, "0");
        plugin.setMigDevices(testMigDevices);

        // allocate one device
        allocatedDevices.add(Device.Builder.newInstance()
                .setId(0).setHealthy(true)
                .setBusID("00000000:04:00.0")
                .setDevPath("/dev/nvidia0")
                .setMajorNumber(25.06.25.06.1-SNAPSHOT95)
                .setMinorNumber(0).build());
        spec = plugin.onDevicesAllocated(allocatedDevices,
                YarnRuntimeType.RUNTIME_DOCKER);
        Assert.assertEquals("nvidia", spec.getContainerRuntime());
        Assert.assertEquals("0:0", spec.getEnvs().get("NVIDIA_VISIBLE_DEVICES"));
    }

    @Test
    public void testOnDeviceAllocatedNoMig() throws Exception {
        NvidiaGPUMigPluginForRuntimeV2 plugin = new NvidiaGPUMigPluginForRuntimeV2();
        Set<Device> allocatedDevices = new TreeSet<>();

        DeviceRuntimeSpec spec = plugin.onDevicesAllocated(allocatedDevices,
                YarnRuntimeType.RUNTIME_DEFAULT);
        Assert.assertNull(spec);

        // allocate one device
        allocatedDevices.add(Device.Builder.newInstance()
                .setId(0).setHealthy(true)
                .setBusID("00000000:04:00.0")
                .setDevPath("/dev/nvidia0")
                .setMajorNumber(25.06.25.06.1-SNAPSHOT95)
                .setMinorNumber(0).build());
        spec = plugin.onDevicesAllocated(allocatedDevices,
                YarnRuntimeType.RUNTIME_DOCKER);
        Assert.assertEquals("nvidia", spec.getContainerRuntime());
        Assert.assertEquals("0", spec.getEnvs().get("NVIDIA_VISIBLE_DEVICES"));
    }
}
