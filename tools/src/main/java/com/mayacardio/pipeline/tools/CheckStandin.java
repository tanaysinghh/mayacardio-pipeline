package com.mayacardio.pipeline.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.mayacardio.pipeline.core.Json;
import com.mayacardio.pipeline.core.LossRange;
import com.mayacardio.pipeline.core.RangeSet;
import com.mayacardio.pipeline.mock.Generator;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public final class CheckStandin {

    private CheckStandin() {
    }

    public static void main(String[] args) throws Exception {
        Path export = null;
        Path gt = null;
        Path report = null;
        Long until = null;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--export" -> export = Path.of(args[++i]);
                case "--gt", "--ground-truth" -> gt = Path.of(args[++i]);
                case "--report" -> report = Path.of(args[++i]);
                case "--until-index" -> until = Long.parseLong(args[++i]);
                default -> throw new IllegalArgumentException("unknown argument " + args[i]);
            }
        }
        if (export == null || gt == null) {
            System.err.println("usage: check-standin --export FILE --gt SESSION_GT.json [--report OUT.json] [--until-index N]");
            System.exit(2);
        }
        Map<String, Object> result = check(ExportData.read(export), Json.read(Files.readString(gt)), until);
        String json = Json.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(result);
        System.out.println(json);
        if (report != null) {
            Files.writeString(report, json + "\n");
        }
        System.exit(Boolean.TRUE.equals(result.get("pass")) ? 0 : 1);
    }

    public static Map<String, Object> check(ExportData d, JsonNode gt, Long untilIndex) {
        Generator gen = new Generator(gt.get("mode").asText(), gt.get("seed").asLong());
        long generated = gt.get("generated").asLong();
        boolean ended = gt.path("ended").asBoolean(false);
        if (!ended && untilIndex == null) {
            generated = Math.max(generated, Math.max(d.maxIndex(1), d.maxIndex(2)) + 1);
        }
        long limit = untilIndex == null ? generated : Math.min(generated, untilIndex + 1);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("tool", "check-standin (stand-in for check_ground_truth)");
        out.put("session_id", gt.get("session_id").asText());
        out.put("generated_per_sensor", generated);
        out.put("ground_truth_note", ended ? "session ended in ground truth" : "ground truth snapshot may lag the mock by up to 200 ms; samples saved after it are still value-checked against the deterministic generator");
        out.put("checked_up_to_index", limit - 1);
        boolean pass = d.malformedLines == 0;
        out.put("malformed_lines", d.malformedLines);
        for (int sensor = 1; sensor <= 2; sensor++) {
            Map<String, Object> s = new LinkedHashMap<>();
            RangeSet outage = new RangeSet();
            for (JsonNode o : gt.get("outages")) {
                if (o.get("sensor").asInt() == sensor) {
                    outage.add(o.get("from_index").asLong(), o.get("to_index").asLong());
                }
            }
            RangeSet lost = new RangeSet();
            long lossSum = 0;
            Map<String, Long> byReason = new TreeMap<>();
            for (LossRange l : d.losses) {
                if (l.sensor() != sensor) {
                    continue;
                }
                long end = Math.min(l.endInclusive(), limit - 1);
                if (end >= l.fromIndex()) {
                    lossSum += end - l.fromIndex() + 1;
                    lost.add(l.fromIndex(), end);
                    byReason.merge(l.reason().isEmpty() ? "(empty reason)" : l.reason(), end - l.fromIndex() + 1, Long::sum);
                }
            }
            long wrong = 0;
            long beyond = 0;
            long contradictions = 0;
            long duringOutage = 0;
            List<String> examples = new ArrayList<>();
            RangeSet silent = new RangeSet();
            for (long i = 0; i < Math.max(limit, d.maxIndex(sensor) + 1); i++) {
                boolean has = i <= d.maxIndex(sensor) && d.present[sensor].get((int) i);
                if (i >= limit) {
                    if (has && untilIndex == null) {
                        beyond++;
                    }
                    continue;
                }
                if (has) {
                    int[] row = d.row(sensor, (int) i);
                    int[] truth = gen.sample(sensor, i);
                    if (!java.util.Arrays.equals(row, truth)) {
                        wrong++;
                        if (examples.size() < 10) {
                            examples.add("index " + i + " got " + java.util.Arrays.toString(row) + " expected " + java.util.Arrays.toString(truth));
                        }
                    }
                    if (lost.contains(i)) {
                        contradictions++;
                    }
                    if (outage.contains(i)) {
                        duringOutage++;
                    }
                } else if (!lost.contains(i)) {
                    silent.add(i, i);
                }
            }
            long overlap = lossSum - lost.count();
            s.put("samples_exported", d.samples[sensor]);
            s.put("duplicates", d.duplicates[sensor]);
            s.put("wrong_values", wrong);
            s.put("beyond_generated", beyond);
            s.put("silent_loss_samples", silent.count());
            s.put("silent_loss_ranges", silent.toList().stream().limit(20).map(r -> r[0] + "-" + r[1]).toList());
            s.put("declared_lost_samples", lost.count());
            s.put("declared_lost_by_reason", byReason);
            s.put("overlapping_loss_samples", overlap);
            s.put("exported_inside_declared_loss", contradictions);
            s.put("exported_during_mock_outage", duringOutage);
            s.put("outage_samples_in_ground_truth", outage.clip(0, limit - 1).count());
            s.put("wrong_value_examples", examples);
            boolean ok = d.duplicates[sensor] == 0 && wrong == 0 && beyond == 0 && silent.isEmpty()
                    && contradictions == 0 && duringOutage == 0 && overlap == 0;
            s.put("pass", ok);
            pass &= ok;
            out.put("sensor_" + sensor, s);
        }
        out.put("pass", pass);
        return out;
    }
}
