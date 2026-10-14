package com.mayacardio.pipeline.cloud;

import com.fasterxml.jackson.databind.JsonNode;
import com.mayacardio.pipeline.cloud.service.IngestService;
import com.mayacardio.pipeline.cloud.service.ProcessingService;
import com.mayacardio.pipeline.cloud.service.SessionRegistry;
import com.mayacardio.pipeline.cloud.service.SessionState;
import com.mayacardio.pipeline.cloud.store.CloudStore;
import com.mayacardio.pipeline.cloud.store.Segment;
import com.mayacardio.pipeline.core.BatchCodec;
import com.mayacardio.pipeline.core.ExportFormat;
import com.mayacardio.pipeline.core.Json;
import com.mayacardio.pipeline.core.LossRange;
import com.mayacardio.pipeline.core.ResultJson;
import com.mayacardio.pipeline.core.Signal;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@RestController
public class ApiController {
    private static final MediaType NDJSON = MediaType.parseMediaType("application/x-ndjson");
    private static final Pattern SESSION_ID = Pattern.compile("[A-Za-z0-9_.-]{1,128}");

    private final SessionRegistry registry;
    private final IngestService ingest;
    private final ProcessingService processing;
    private final CloudStore store;
    private final String startedAt = Instant.now().toString();

    public ApiController(SessionRegistry registry, IngestService ingest, ProcessingService processing, CloudStore store) {
        this.registry = registry;
        this.ingest = ingest;
        this.processing = processing;
        this.store = store;
    }

    private static ResponseEntity<String> json(Object o) {
        return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(Json.write(o));
    }

    private static String checkSession(String id) {
        if (!SESSION_ID.matcher(id).matches()) {
            throw new IllegalArgumentException("bad session id");
        }
        return id;
    }

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON)
                .body("{\"status\": \"ok\", \"time\": \"" + Instant.now() + "\"}");
    }

    @GetMapping("/")
    public ResponseEntity<String> index() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("service", "mayacardio pipeline cloud");
        m.put("storage", store.backend());
        m.put("started_at", startedAt);
        m.put("windows_computed_since_start", processing.windowsComputed());
        return json(m);
    }

    @PostMapping(value = "/api/v1/ingest", consumes = MediaType.ALL_VALUE)
    public ResponseEntity<String> ingest(@RequestBody byte[] body) throws IOException {
        BatchCodec.Batch batch = BatchCodec.decode(body);
        checkSession(batch.sessionId());
        IngestService.IngestResult r = ingest.ingest(batch);
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("session", r.sessionId());
        m.put("new", r.newSamples());
        m.put("duplicates", r.duplicates());
        m.put("conflicts", r.conflicts());
        m.put("held_1", r.held1());
        m.put("held_2", r.held2());
        return json(m);
    }

    @GetMapping("/api/v1/sessions")
    public ResponseEntity<String> sessions() {
        List<Map<String, Object>> out = new ArrayList<>();
        for (CloudStore.SessionMeta m : store.sessions()) {
            Map<String, Object> e = new LinkedHashMap<>();
            e.put("session", m.sessionId());
            e.put("created_at", m.createdAt());
            e.put("closed", m.closed());
            out.add(e);
        }
        return json(out);
    }

    @GetMapping("/api/v1/sessions/{id}/ranges")
    public ResponseEntity<String> ranges(@PathVariable String id) {
        SessionState st = registry.get(checkSession(id));
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("session", id);
        for (int s = 1; s <= 2; s++) {
            Map<String, Object> e = new LinkedHashMap<>();
            e.put("held", st.heldCount(s));
            e.put("ranges", st.heldRanges(s));
            m.put("sensor_" + s, e);
        }
        return json(m);
    }

    @PostMapping(value = "/api/v1/sessions/{id}/losses", consumes = MediaType.ALL_VALUE)
    public ResponseEntity<String> losses(@PathVariable String id, @RequestBody String body) {
        checkSession(id);
        JsonNode arr = Json.read(body);
        List<LossRange> losses = new ArrayList<>();
        for (JsonNode n : arr) {
            int sensor = n.get("sensor").asInt();
            if (!Signal.validSensor(sensor)) {
                throw new IllegalArgumentException("bad sensor");
            }
            JsonNode to = n.get("to_index");
            losses.add(new LossRange(sensor, n.get("from_index").asLong(),
                    to == null || to.isNull() ? null : to.asLong(), n.path("reason").asText("unspecified")));
        }
        int n = ingest.addLosses(id, losses);
        return json(Map.of("session", id, "losses", n));
    }

    @PostMapping(value = "/api/v1/sessions/{id}/meta", consumes = MediaType.ALL_VALUE)
    public ResponseEntity<String> meta(@PathVariable String id, @RequestBody String body) {
        checkSession(id);
        JsonNode n = Json.read(body);
        Long n1 = n.hasNonNull("final_next_1") ? n.get("final_next_1").asLong() : null;
        Long n2 = n.hasNonNull("final_next_2") ? n.get("final_next_2").asLong() : null;
        ingest.setMeta(id, n.path("closed").asBoolean(false), n1, n2, n.path("close_reason").asText(null));
        return json(Map.of("session", id, "ok", true));
    }

    @PostMapping(value = "/api/v1/sessions/{id}/results", consumes = MediaType.ALL_VALUE)
    public ResponseEntity<String> uploadResults(@PathVariable String id, @RequestBody String body) {
        checkSession(id);
        registry.get(id);
        List<CloudStore.StoredResult> results = new ArrayList<>();
        for (String line : body.split("\n")) {
            if (line.isBlank()) {
                continue;
            }
            JsonNode n = Json.read(line);
            if (!id.equals(n.path("session").asText()) || !ResultJson.OFFLINE.equals(n.path("mode").asText())) {
                throw new IllegalArgumentException("only offline results of this session may be uploaded");
            }
            ResultJson.parse(n);
            results.add(new CloudStore.StoredResult(n.get("window").asLong(), ResultJson.OFFLINE, line.trim()));
        }
        store.upsertResults(id, results, Instant.now().toString());
        return json(Map.of("session", id, "stored", results.size()));
    }

    @GetMapping("/api/v1/sessions/{id}/results")
    public ResponseEntity<StreamingResponseBody> results(@PathVariable String id, @RequestParam(required = false) String mode) {
        checkSession(id);
        StreamingResponseBody body = out -> {
            BufferedWriter w = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8), 65536);
            store.forEachResult(id, mode, r -> write(w, r.body()));
            w.flush();
        };
        return ResponseEntity.ok().contentType(NDJSON).body(body);
    }

    @GetMapping("/api/v1/sessions/{id}/export")
    public ResponseEntity<StreamingResponseBody> export(@PathVariable String id) {
        checkSession(id);
        if (store.meta(id) == null) {
            return ResponseEntity.notFound().build();
        }
        StreamingResponseBody body = out -> {
            BufferedWriter w = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8), 1 << 16);
            for (int s = 1; s <= 2; s++) {
                int sensor = s;
                store.forEachSegment(id, sensor, summary -> {
                    Segment seg = Segment.decode(summary.seg(), summary.data());
                    for (int i = 0; i < Segment.SIZE; i++) {
                        if (seg.has(i)) {
                            write(w, ExportFormat.sampleLine(sensor, seg.firstIndex() + i, seg.row(i)));
                        }
                    }
                });
            }
            for (LossRange l : store.losses(id)) {
                write(w, ExportFormat.lossLine(l));
            }
            w.flush();
        };
        return ResponseEntity.ok().contentType(NDJSON).body(body);
    }

    private static void write(BufferedWriter w, String line) {
        try {
            w.write(line);
            w.write('\n');
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @GetMapping("/api/v1/sessions/{id}/status")
    public ResponseEntity<String> status(@PathVariable String id) {
        checkSession(id);
        SessionState st = registry.existing(id);
        if (st == null) {
            return ResponseEntity.notFound().build();
        }
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("session", id);
        m.put("held_1", st.heldCount(1));
        m.put("held_2", st.heldCount(2));
        m.put("closed", st.closed());
        m.put("online_windows_computed", st.computedCount());
        m.put("windows_pending", st.pendingCount());
        m.put("conflicts", st.conflicts());
        m.put("last_ingest_at", st.lastIngestWall());
        return json(m);
    }

    @DeleteMapping("/api/v1/sessions/{id}")
    public ResponseEntity<String> delete(@PathVariable String id) {
        checkSession(id);
        store.deleteSession(id);
        registry.forget(id);
        return json(Map.of("session", id, "deleted", true));
    }

    @ExceptionHandler({IllegalArgumentException.class, IOException.class})
    public ResponseEntity<String> badRequest(Exception e) {
        return ResponseEntity.badRequest().contentType(MediaType.APPLICATION_JSON)
                .body("{\"error\": " + Json.quote(String.valueOf(e.getMessage())) + "}");
    }
}
