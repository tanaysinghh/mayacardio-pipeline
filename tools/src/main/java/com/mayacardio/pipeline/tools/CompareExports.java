package com.mayacardio.pipeline.tools;

import com.mayacardio.pipeline.core.Json;
import com.mayacardio.pipeline.core.LossRange;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public final class CompareExports {

    private CompareExports() {
    }

    public static void main(String[] args) throws Exception {
        Path a = null;
        Path b = null;
        Path report = null;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--device", "--a" -> a = Path.of(args[++i]);
                case "--cloud", "--b" -> b = Path.of(args[++i]);
                case "--report" -> report = Path.of(args[++i]);
                default -> throw new IllegalArgumentException("unknown argument " + args[i]);
            }
        }
        Map<String, Object> r = compare(ExportData.read(a), ExportData.read(b));
        String json = Json.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(r);
        System.out.println(json);
        if (report != null) {
            Files.writeString(report, json + "\n");
        }
        System.exit(Boolean.TRUE.equals(r.get("identical")) ? 0 : 1);
    }

    public static Map<String, Object> compare(ExportData device, ExportData cloud) {
        Map<String, Object> out = new LinkedHashMap<>();
        boolean identical = device.malformedLines == 0 && cloud.malformedLines == 0;
        for (int sensor = 1; sensor <= 2; sensor++) {
            long missing = 0;
            long extra = 0;
            long mismatch = 0;
            long max = Math.max(device.maxIndex(sensor), cloud.maxIndex(sensor));
            for (int i = 0; i <= max; i++) {
                boolean inD = device.present[sensor].get(i);
                boolean inC = cloud.present[sensor].get(i);
                if (inD && !inC) {
                    missing++;
                } else if (!inD && inC) {
                    extra++;
                } else if (inD && !java.util.Arrays.equals(device.row(sensor, i), cloud.row(sensor, i))) {
                    mismatch++;
                }
            }
            Map<String, Object> s = new LinkedHashMap<>();
            s.put("device_samples", device.samples[sensor]);
            s.put("cloud_samples", cloud.samples[sensor]);
            s.put("missing_in_cloud", missing);
            s.put("extra_in_cloud", extra);
            s.put("value_mismatches", mismatch);
            s.put("device_duplicates", device.duplicates[sensor]);
            s.put("cloud_duplicates", cloud.duplicates[sensor]);
            identical &= missing == 0 && extra == 0 && mismatch == 0 && device.duplicates[sensor] == 0 && cloud.duplicates[sensor] == 0;
            out.put("sensor_" + sensor, s);
        }
        Set<LossRange> dl = new HashSet<>(device.losses);
        Set<LossRange> cl = new HashSet<>(cloud.losses);
        out.put("device_losses", dl.size());
        out.put("cloud_losses", cl.size());
        out.put("losses_equal", dl.equals(cl));
        identical &= dl.equals(cl);
        out.put("identical", identical);
        return out;
    }
}
