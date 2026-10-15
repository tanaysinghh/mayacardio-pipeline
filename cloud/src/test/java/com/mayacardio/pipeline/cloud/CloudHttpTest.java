package com.mayacardio.pipeline.cloud;

import com.fasterxml.jackson.databind.JsonNode;
import com.mayacardio.pipeline.core.BatchCodec;
import com.mayacardio.pipeline.core.Json;
import com.mayacardio.pipeline.core.SampleRun;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CloudHttpTest {
    private final HttpClient http = HttpClient.newHttpClient();

    private HttpResponse<String> send(HttpRequest.Builder b) throws Exception {
        return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void endToEndOverHttp(@TempDir Path dir) throws Exception {
        try (CloudMain app = CloudMain.start(Map.of("PORT", "0", "CLOUD_SQLITE_PATH", dir.resolve("c.db").toString(), "CLOUD_API_TOKEN", "tok"))) {
            String base = "http://127.0.0.1:" + app.port();
            assertEquals(200, send(HttpRequest.newBuilder(URI.create(base + "/health"))).statusCode());
            assertEquals(401, send(HttpRequest.newBuilder(URI.create(base + "/api/v1/sessions"))).statusCode());
            List<SampleRun> runs = new ArrayList<>();
            for (int s = 1; s <= 2; s++) {
                int[][] rows = new int[2401][];
                for (int i = 0; i < rows.length; i++) {
                    rows[i] = s == 1 ? new int[]{i, 2 * i, 3 * i} : new int[]{1000 + i, 2000 + i};
                }
                runs.add(new SampleRun(s, 0, rows));
            }
            byte[] body = BatchCodec.encode(new BatchCodec.Batch("h1", 1, runs));
            HttpResponse<String> ing = send(HttpRequest.newBuilder(URI.create(base + "/api/v1/ingest"))
                    .header("Authorization", "Bearer tok").POST(HttpRequest.BodyPublishers.ofByteArray(body)));
            assertEquals(200, ing.statusCode(), ing.body());
            assertEquals(2401, Json.read(ing.body()).get("held_1").asLong());
            HttpResponse<String> again = send(HttpRequest.newBuilder(URI.create(base + "/api/v1/ingest"))
                    .header("Authorization", "Bearer tok").POST(HttpRequest.BodyPublishers.ofByteArray(body)));
            assertEquals(2401 * 2, Json.read(again.body()).get("duplicates").asLong());
            JsonNode ranges = Json.read(send(HttpRequest.newBuilder(URI.create(base + "/api/v1/sessions/h1/ranges"))
                    .header("Authorization", "Bearer tok")).body());
            assertEquals("[[0,2400]]", ranges.get("sensor_2").get("ranges").toString());
            assertEquals(200, send(HttpRequest.newBuilder(URI.create(base + "/api/v1/sessions/h1/losses")).header("Authorization", "Bearer tok")
                    .POST(HttpRequest.BodyPublishers.ofString("[{\"sensor\": 1, \"from_index\": 2401, \"to_index\": null, \"reason\": \"x\"}]"))).statusCode());
            assertEquals(200, send(HttpRequest.newBuilder(URI.create(base + "/api/v1/sessions/h1/meta")).header("Authorization", "Bearer tok")
                    .POST(HttpRequest.BodyPublishers.ofString("{\"closed\": true, \"final_next_1\": 2401, \"final_next_2\": 2401}"))).statusCode());
            String results = "";
            for (int k = 0; k < 50 && results.split("\n").length < 6; k++) {
                Thread.sleep(100);
                results = send(HttpRequest.newBuilder(URI.create(base + "/api/v1/sessions/h1/results?mode=online"))
                        .header("Authorization", "Bearer tok")).body();
            }
            assertEquals(7, results.strip().split("\n").length, results);
            String export = send(HttpRequest.newBuilder(URI.create(base + "/api/v1/sessions/h1/export")).header("Authorization", "Bearer tok")).body();
            String[] lines = export.strip().split("\n");
            assertEquals(2401 * 2 + 1, lines.length);
            assertEquals("{\"sensor\": 1, \"index\": 0, \"values\": [0, 0, 0]}", lines[0]);
            assertTrue(lines[lines.length - 1].contains("\"to_index\": null"));
            assertEquals(404, send(HttpRequest.newBuilder(URI.create(base + "/api/v1/sessions/zz/export")).header("Authorization", "Bearer tok")).statusCode());
            assertEquals(400, send(HttpRequest.newBuilder(URI.create(base + "/api/v1/ingest")).header("Authorization", "Bearer tok")
                    .POST(HttpRequest.BodyPublishers.ofString("garbage"))).statusCode());
            assertTrue(send(HttpRequest.newBuilder(URI.create(base + "/"))).body().contains("\"interpretation\""));
        }
    }

    @Test
    void deleteNeedsTheTokenAndRemovesOnlyThatSession(@TempDir Path dir) throws Exception {
        try (CloudMain app = CloudMain.start(Map.of("PORT", "0", "CLOUD_SQLITE_PATH", dir.resolve("c.db").toString(), "CLOUD_API_TOKEN", "tok"))) {
            String base = "http://127.0.0.1:" + app.port();
            for (String id : new String[]{"keep", "drop"}) {
                byte[] body = BatchCodec.encode(new BatchCodec.Batch(id, 1, List.of(new SampleRun(1, 0, new int[][]{{1, 2, 3}, {4, 5, 6}}))));
                assertEquals(200, send(HttpRequest.newBuilder(URI.create(base + "/api/v1/ingest"))
                        .header("Authorization", "Bearer tok").POST(HttpRequest.BodyPublishers.ofByteArray(body))).statusCode());
            }
            assertEquals(401, send(HttpRequest.newBuilder(URI.create(base + "/api/v1/sessions/drop")).DELETE()).statusCode());
            assertEquals(401, send(HttpRequest.newBuilder(URI.create(base + "/api/v1/sessions/drop"))
                    .header("Authorization", "Bearer wrong").DELETE()).statusCode());
            assertEquals(400, send(HttpRequest.newBuilder(URI.create(base + "/api/v1/sessions/a%20b"))
                    .header("Authorization", "Bearer tok").DELETE()).statusCode());
            HttpResponse<String> del = send(HttpRequest.newBuilder(URI.create(base + "/api/v1/sessions/drop"))
                    .header("Authorization", "Bearer tok").DELETE());
            assertEquals(200, del.statusCode(), del.body());
            assertEquals(404, send(HttpRequest.newBuilder(URI.create(base + "/api/v1/sessions/drop/export")).header("Authorization", "Bearer tok")).statusCode());
            String sessions = send(HttpRequest.newBuilder(URI.create(base + "/api/v1/sessions")).header("Authorization", "Bearer tok")).body();
            assertTrue(sessions.contains("\"keep\"") && !sessions.contains("\"drop\""), sessions);
            String kept = send(HttpRequest.newBuilder(URI.create(base + "/api/v1/sessions/keep/export")).header("Authorization", "Bearer tok")).body();
            assertEquals(2, kept.strip().split("\n").length, kept);
        }
    }
}
