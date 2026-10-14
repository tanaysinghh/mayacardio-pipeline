package com.mayacardio.pipeline.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.mayacardio.pipeline.core.Json;
import com.mayacardio.pipeline.core.LossRange;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

public final class ExportData {
    public final BitSet[] present = {null, new BitSet(), new BitSet()};
    public final int[][][] values = {null, new int[3][0], new int[2][0]};
    public final long[] duplicates = new long[3];
    public final long[] duplicateMismatches = new long[3];
    public final long[] samples = new long[3];
    public final List<LossRange> losses = new ArrayList<>();
    public long malformedLines;

    public static ExportData read(Path file) throws IOException {
        ExportData d = new ExportData();
        try (BufferedReader r = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            String line;
            while ((line = r.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                JsonNode n;
                try {
                    n = Json.read(line);
                } catch (IllegalArgumentException e) {
                    d.malformedLines++;
                    continue;
                }
                int sensor = n.path("sensor").asInt(-1);
                if (sensor != 1 && sensor != 2) {
                    d.malformedLines++;
                    continue;
                }
                if (n.has("index")) {
                    JsonNode vals = n.get("values");
                    int[] row = new int[vals.size()];
                    for (int c = 0; c < row.length; c++) {
                        row[c] = vals.get(c).asInt();
                    }
                    d.addSample(sensor, n.get("index").asLong(), row);
                } else if (n.has("from_index")) {
                    JsonNode to = n.get("to_index");
                    d.losses.add(new LossRange(sensor, n.get("from_index").asLong(),
                            to == null || to.isNull() ? null : to.asLong(), n.path("reason").asText("")));
                } else {
                    d.malformedLines++;
                }
            }
        }
        return d;
    }

    void addSample(int sensor, long index, int[] row) {
        int i = (int) index;
        int channels = sensor == 1 ? 3 : 2;
        if (row.length != channels || index < 0 || index > Integer.MAX_VALUE - 1) {
            malformedLines++;
            return;
        }
        ensure(sensor, i + 1);
        if (present[sensor].get(i)) {
            duplicates[sensor]++;
            for (int c = 0; c < channels; c++) {
                if (values[sensor][c][i] != row[c]) {
                    duplicateMismatches[sensor]++;
                    break;
                }
            }
            return;
        }
        present[sensor].set(i);
        for (int c = 0; c < channels; c++) {
            values[sensor][c][i] = row[c];
        }
        samples[sensor]++;
    }

    private void ensure(int sensor, int size) {
        if (values[sensor][0].length >= size) {
            return;
        }
        int n = Math.max(size, values[sensor][0].length * 2 + 1024);
        for (int c = 0; c < values[sensor].length; c++) {
            values[sensor][c] = Arrays.copyOf(values[sensor][c], n);
        }
    }

    public int[] row(int sensor, int i) {
        int[] r = new int[values[sensor].length];
        for (int c = 0; c < r.length; c++) {
            r[c] = values[sensor][c][i];
        }
        return r;
    }

    public long maxIndex(int sensor) {
        return present[sensor].length() - 1;
    }
}
