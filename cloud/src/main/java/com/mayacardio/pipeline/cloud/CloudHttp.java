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
import com.mayacardio.pipeline.core.Interpretation;
import com.mayacardio.pipeline.core.Json;
import com.mayacardio.pipeline.core.LossRange;
import com.mayacardio.pipeline.core.ResultJson;
import com.mayacardio.pipeline.core.Signal;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public final class CloudHttp implements HttpHandler {
    private static final Logger LOG = Logger.getLogger(CloudHttp.class.getName());
    private static final Pattern SESSION_ID = Pattern.compile("[A-Za-z0-9_.-]{1,128}");
    private static final int MAX_BODY = 64 * 1024 * 1024;

    private final SessionRegistry registry;
    private final IngestService ingest;
    private final ProcessingService processing;
    private final CloudStore store;
    private final byte[] expectedAuth;
    private final String startedAt = Instant.now().toString();

    public CloudHttp(SessionRegistry registry, IngestService ingest, ProcessingService processing, CloudStore store, String token) {
        this.registry = registry;
        this.ingest = ingest;
        this.processing = processing;
        this.store = store;
        this.expectedAuth = token == null || token.isBlank() ? null : ("Bearer " + token.trim()).getBytes(StandardCharsets.UTF_8);
    }

    private static final class HttpError extends RuntimeException {
        final int status;

        HttpError(int status, String message) {
            super(message);
            this.status = status;
        }
    }

    @Override
    public void handle(HttpExchange ex) throws IOException {
        try {
            route(ex);
        } catch (HttpError e) {
            sendJson(ex, e.status, "{\"error\": " + Json.quote(e.getMessage()) + "}");
        } catch (IllegalArgumentException | IOException e) {
            sendJson(ex, 400, "{\"error\": " + Json.quote(String.valueOf(e.getMessage())) + "}");
        } catch (RuntimeException e) {
            LOG.log(Level.WARNING, "request failed: " + ex.getRequestURI(), e);
            sendJson(ex, 500, "{\"error\": \"internal error\"}");
        } finally {
            ex.close();
        }
    }

    private void route(HttpExchange ex) throws IOException {
        String method = ex.getRequestMethod();
        String path = ex.getRequestURI().getPath();
        if (path.equals("/health") && method.equals("GET")) {
            sendJson(ex, 200, "{\"status\": \"ok\", \"time\": \"" + Instant.now() + "\"}");
            return;
        }
        if (path.equals("/") && method.equals("GET")) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("service", "mayacardio pipeline cloud");
            m.put("storage", store.backend());
            m.put("interpretation", Interpretation.describe());
            m.put("started_at", startedAt);
            m.put("windows_computed_since_start", processing.windowsComputed());
            sendJson(ex, 200, Json.write(m));
            return;
        }
        if (!path.startsWith("/api/v1/")) {
            throw new HttpError(404, "not found");
        }
        if (expectedAuth != null) {
            String header = ex.getRequestHeaders().getFirst("Authorization");
            byte[] got = header == null ? new byte[0] : header.getBytes(StandardCharsets.UTF_8);
            if (!MessageDigest.isEqual(expectedAuth, got)) {
                throw new HttpError(401, "unauthorized");
            }
        }
        String[] p = path.substring("/api/v1/".length()).split("/");
        if (p.length == 1 && p[0].equals("ingest") && method.equals("POST")) {
            ingest(ex);
        } else if (p.length == 1 && p[0].equals("sessions") && method.equals("GET")) {
            sessions(ex);
        } else if (p.length == 2 && p[0].equals("sessions") && method.equals("DELETE")) {
            String id = checkSession(p[1]);
            store.deleteSession(id);
            registry.forget(id);
            sendJson(ex, 200, Json.write(Map.of("session", id, "deleted", true)));
        } else if (p.length == 3 && p[0].equals("sessions")) {
            String id = checkSession(p[1]);
            switch (method + " " + p[2]) {
                case "GET ranges" -> ranges(ex, id);
                case "POST losses" -> losses(ex, id);
                case "POST meta" -> meta(ex, id);
                case "POST results" -> uploadResults(ex, id);
                case "GET results" -> results(ex, id, query(ex, "mode"));
                case "GET export" -> export(ex, id);
                case "GET status" -> status(ex, id);
                default -> throw new HttpError(404, "not found");
            }
        } else {
            throw new HttpError(404, "not found");
        }
    }

    private static String checkSession(String id) {
        if (!SESSION_ID.matcher(id).matches()) {
            throw new IllegalArgumentException("bad session id");
        }
        return id;
    }

    private static String query(HttpExchange ex, String key) {
        String q = ex.getRequestURI().getRawQuery();
        if (q == null) {
            return null;
        }
        for (String kv : q.split("&")) {
            int i = kv.indexOf('=');
            if (i > 0 && kv.substring(0, i).equals(key)) {
                return URLDecoder.decode(kv.substring(i + 1), StandardCharsets.UTF_8);
            }
        }
        return null;
    }

    private static byte[] body(HttpExchange ex) throws IOException {
        try (InputStream in = ex.getRequestBody()) {
            ByteArrayOutputStream out = new ByteArrayOutputStream(16384);
            byte[] buf = new byte[16384];
            int n;
            while ((n = in.read(buf)) > 0) {
                out.write(buf, 0, n);
                if (out.size() > MAX_BODY) {
                    throw new HttpError(413, "body too large");
                }
            }
            return out.toByteArray();
        }
    }

    private static void sendJson(HttpExchange ex, int status, String json) throws IOException {
        byte[] b = json.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(status, b.length);
        try (OutputStream out = ex.getResponseBody()) {
            out.write(b);
        }
    }

    private interface LineSource {
        void write(BufferedWriter w) throws IOException;
    }

    private static void sendLines(HttpExchange ex, LineSource source) throws IOException {
        ex.getResponseHeaders().set("Content-Type", "application/x-ndjson");
        ex.sendResponseHeaders(200, 0);
        try (BufferedWriter w = new BufferedWriter(new OutputStreamWriter(ex.getResponseBody(), StandardCharsets.UTF_8), 1 << 16)) {
            source.write(w);
        }
    }

    private static void line(BufferedWriter w, String s) {
        try {
            w.write(s);
            w.write('\n');
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void ingest(HttpExchange ex) throws IOException {
        BatchCodec.Batch batch = BatchCodec.decode(body(ex));
        checkSession(batch.sessionId());
        IngestService.IngestResult r = ingest.ingest(batch);
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("session", r.sessionId());
        m.put("new", r.newSamples());
        m.put("duplicates", r.duplicates());
        m.put("conflicts", r.conflicts());
        m.put("held_1", r.held1());
        m.put("held_2", r.held2());
        sendJson(ex, 200, Json.write(m));
    }

    private void sessions(HttpExchange ex) throws IOException {
        List<Map<String, Object>> out = new ArrayList<>();
        for (CloudStore.SessionMeta m : store.sessions()) {
            Map<String, Object> e = new LinkedHashMap<>();
            e.put("session", m.sessionId());
            e.put("created_at", m.createdAt());
            e.put("closed", m.closed());
            out.add(e);
        }
        sendJson(ex, 200, Json.write(out));
    }

    private void ranges(HttpExchange ex, String id) throws IOException {
        SessionState st = registry.get(id);
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("session", id);
        for (int s = 1; s <= 2; s++) {
            Map<String, Object> e = new LinkedHashMap<>();
            e.put("held", st.heldCount(s));
            e.put("ranges", st.heldRanges(s));
            m.put("sensor_" + s, e);
        }
        sendJson(ex, 200, Json.write(m));
    }

    private void losses(HttpExchange ex, String id) throws IOException {
        JsonNode arr = Json.read(new String(body(ex), StandardCharsets.UTF_8));
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
        sendJson(ex, 200, Json.write(Map.of("session", id, "losses", n)));
    }

    private void meta(HttpExchange ex, String id) throws IOException {
        JsonNode n = Json.read(new String(body(ex), StandardCharsets.UTF_8));
        Long n1 = n.hasNonNull("final_next_1") ? n.get("final_next_1").asLong() : null;
        Long n2 = n.hasNonNull("final_next_2") ? n.get("final_next_2").asLong() : null;
        ingest.setMeta(id, n.path("closed").asBoolean(false), n1, n2, n.path("close_reason").asText(null));
        sendJson(ex, 200, Json.write(Map.of("session", id, "ok", true)));
    }

    private void uploadResults(HttpExchange ex, String id) throws IOException {
        registry.get(id);
        List<CloudStore.StoredResult> results = new ArrayList<>();
        for (String l : new String(body(ex), StandardCharsets.UTF_8).split("\n")) {
            if (l.isBlank()) {
                continue;
            }
            JsonNode n = Json.read(l);
            if (!id.equals(n.path("session").asText()) || !ResultJson.OFFLINE.equals(n.path("mode").asText())) {
                throw new IllegalArgumentException("only offline results of this session may be uploaded");
            }
            ResultJson.parse(n);
            results.add(new CloudStore.StoredResult(n.get("window").asLong(), ResultJson.OFFLINE, l.trim()));
        }
        store.upsertResults(id, results, Instant.now().toString());
        sendJson(ex, 200, Json.write(Map.of("session", id, "stored", results.size())));
    }

    private void results(HttpExchange ex, String id, String mode) throws IOException {
        sendLines(ex, w -> store.forEachResult(id, mode, r -> line(w, r.body())));
    }

    private void export(HttpExchange ex, String id) throws IOException {
        if (store.meta(id) == null) {
            throw new HttpError(404, "unknown session");
        }
        sendLines(ex, w -> {
            for (int s = 1; s <= 2; s++) {
                int sensor = s;
                store.forEachSegment(id, sensor, summary -> {
                    Segment seg = Segment.decode(summary.seg(), summary.data());
                    for (int i = 0; i < Segment.SIZE; i++) {
                        if (seg.has(i)) {
                            line(w, ExportFormat.sampleLine(sensor, seg.firstIndex() + i, seg.row(i)));
                        }
                    }
                });
            }
            for (LossRange l : store.losses(id)) {
                line(w, ExportFormat.lossLine(l));
            }
        });
    }

    private void status(HttpExchange ex, String id) throws IOException {
        SessionState st = registry.existing(id);
        if (st == null) {
            throw new HttpError(404, "unknown session");
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
        sendJson(ex, 200, Json.write(m));
    }
}
