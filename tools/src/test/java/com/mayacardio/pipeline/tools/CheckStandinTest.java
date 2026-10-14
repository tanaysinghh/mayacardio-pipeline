package com.mayacardio.pipeline.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.mayacardio.pipeline.core.ExportFormat;
import com.mayacardio.pipeline.core.Json;
import com.mayacardio.pipeline.core.LossRange;
import com.mayacardio.pipeline.mock.Generator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CheckStandinTest {

    private static JsonNode gt(long generated) {
        return Json.read("{\"session_id\": \"abc\", \"mode\": \"ramp\", \"seed\": 7, \"generated\": " + generated
                + ", \"outages\": [{\"sensor\": 2, \"from_index\": 100, \"to_index\": 199}]}");
    }

    private static Path write(Path dir, List<String> lines) throws Exception {
        Path p = dir.resolve("export.jsonl");
        Files.write(p, lines);
        return p;
    }

    @Test
    void passesWhenEverySampleIsExportedOrDeclaredLost(@TempDir Path dir) throws Exception {
        Generator g = new Generator("ramp", 7);
        List<String> lines = new ArrayList<>();
        for (int s = 1; s <= 2; s++) {
            for (long i = 0; i < 1000; i++) {
                if (s == 2 && i >= 100 && i <= 199) {
                    continue;
                }
                if (s == 1 && i >= 990) {
                    continue;
                }
                lines.add(ExportFormat.sampleLine(s, i, g.sample(s, i)));
            }
        }
        lines.add(ExportFormat.lossLine(new LossRange(2, 100, 199L, "sensor_outage")));
        lines.add(ExportFormat.lossLine(new LossRange(1, 990, null, "session_restart")));
        Map<String, Object> r = CheckStandin.check(ExportData.read(write(dir, lines)), gt(1000), null);
        assertTrue((Boolean) r.get("pass"), r.toString());
    }

    @Test
    void detectsSilentLossDuplicatesAndWrongValues(@TempDir Path dir) throws Exception {
        Generator g = new Generator("ramp", 7);
        List<String> lines = new ArrayList<>();
        for (long i = 0; i < 1000; i++) {
            if (i == 500) {
                continue;
            }
            int[] v = g.sample(1, i);
            if (i == 600) {
                v[0]++;
            }
            lines.add(ExportFormat.sampleLine(1, i, v));
            if (i == 700) {
                lines.add(ExportFormat.sampleLine(1, i, v));
            }
        }
        Map<String, Object> r = CheckStandin.check(ExportData.read(write(dir, lines)), gt(1000), null);
        assertFalse((Boolean) r.get("pass"));
        @SuppressWarnings("unchecked")
        Map<String, Object> s1 = (Map<String, Object>) r.get("sensor_1");
        assertEquals(1L, s1.get("silent_loss_samples"));
        assertEquals(1L, s1.get("duplicates"));
        assertEquals(1L, s1.get("wrong_values"));
    }
}
