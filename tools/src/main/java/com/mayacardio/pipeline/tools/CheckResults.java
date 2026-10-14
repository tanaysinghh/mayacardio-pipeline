package com.mayacardio.pipeline.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.mayacardio.pipeline.core.Json;
import com.mayacardio.pipeline.core.LossRange;

import java.io.BufferedReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public final class CheckResults {
    private static final String[] FIELDS = {"missing_1", "missing_2", "complete", "mean_1", "min_1", "max_1",
            "mean_2", "min_2", "max_2", "magnitude", "heart_rate"};

    private CheckResults() {
    }

    public static void main(String[] args) throws Exception {
        Path export = null;
        Path results = null;
        Path report = null;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--export" -> export = Path.of(args[++i]);
                case "--results" -> results = Path.of(args[++i]);
                case "--report" -> report = Path.of(args[++i]);
                default -> throw new IllegalArgumentException("unknown argument " + args[i]);
            }
        }
        Map<String, Object> r = check(ExportData.read(export), results);
        String json = Json.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(r);
        System.out.println(json);
        if (report != null) {
            Files.writeString(report, json + "\n");
        }
        System.exit(Boolean.TRUE.equals(r.get("pass")) ? 0 : 1);
    }

    static long knownThrough(ExportData d, int sensor) {
        long k = d.maxIndex(sensor);
        for (LossRange l : d.losses) {
            if (l.sensor() == sensor) {
                k = Math.max(k, l.endInclusive());
            }
        }
        return k;
    }

    public static Map<String, Object> check(ExportData data, Path results) throws Exception {
        ReferenceProcessor ref = new ReferenceProcessor(data);
        long known1 = knownThrough(data, 1);
        long known2 = knownThrough(data, 2);
        Map<String, long[]> perMode = new TreeMap<>();
        List<String> examples = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        long duplicates = 0;
        try (BufferedReader r = Files.newBufferedReader(results, StandardCharsets.UTF_8)) {
            String line;
            while ((line = r.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                JsonNode n = Json.read(line);
                long w = n.get("window").asLong();
                String mode = n.path("mode").asText();
                long[] c = perMode.computeIfAbsent(mode, k -> new long[4]);
                c[0]++;
                if (!seen.add(mode + ":" + w)) {
                    duplicates++;
                }
                if (400 * (w + 1) > known1 || 400 * w + 399 > known2) {
                    c[3]++;
                    continue;
                }
                Map<String, Object> expected = ref.window(w);
                List<String> diffs = new ArrayList<>();
                for (String f : FIELDS) {
                    if (!same(n.get(f), expected.get(f))) {
                        diffs.add(f + ": got " + n.get(f) + " expected " + expected.get(f));
                    }
                }
                if (diffs.isEmpty()) {
                    c[1]++;
                } else {
                    c[2]++;
                    if (examples.size() < 20) {
                        examples.add("window " + w + " (" + mode + "): " + String.join("; ", diffs));
                    }
                }
            }
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("reference", "naive independent implementation in tools (stand-in for the official reference)");
        boolean pass = duplicates == 0;
        for (Map.Entry<String, long[]> e : perMode.entrySet()) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("results", e.getValue()[0]);
            m.put("matched", e.getValue()[1]);
            m.put("mismatched", e.getValue()[2]);
            m.put("not_verifiable_beyond_export", e.getValue()[3]);
            out.put(e.getKey(), m);
            pass &= e.getValue()[2] == 0;
        }
        out.put("duplicate_window_mode_lines", duplicates);
        out.put("mismatch_examples", examples);
        out.put("pass", pass);
        return out;
    }

    @SuppressWarnings("unchecked")
    static boolean same(JsonNode got, Object exp) {
        if (got == null || got.isNull()) {
            return exp == null;
        }
        if (exp == null) {
            return false;
        }
        if (exp instanceof List<?> list) {
            if (!got.isArray() || got.size() != list.size()) {
                return false;
            }
            for (int i = 0; i < list.size(); i++) {
                if (!same(got.get(i), list.get(i))) {
                    return false;
                }
            }
            return true;
        }
        if (exp instanceof Boolean b) {
            return got.isBoolean() && got.asBoolean() == b;
        }
        if (exp instanceof String s) {
            return got.isNumber() && got.decimalValue().compareTo(new BigDecimal(s)) == 0;
        }
        if (exp instanceof Number num) {
            return got.isNumber() && got.decimalValue().compareTo(new BigDecimal(num.toString())) == 0;
        }
        return false;
    }
}
