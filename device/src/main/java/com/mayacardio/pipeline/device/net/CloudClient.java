package com.mayacardio.pipeline.device.net;

import com.fasterxml.jackson.databind.JsonNode;
import com.mayacardio.pipeline.core.ExportFormat;
import com.mayacardio.pipeline.core.Json;
import com.mayacardio.pipeline.core.LossRange;
import com.mayacardio.pipeline.core.RangeSet;
import com.mayacardio.pipeline.device.Clocks;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public final class CloudClient {
    private static final int HEADER_ESTIMATE = 350;

    public static final class CloudException extends IOException {
        public CloudException(String message) {
            super(message);
        }
    }

    public record Ranges(RangeSet[] held, long[] heldCount) {
    }

    public record Timing(int bytes, int attempts, long connectMs, long firstByteMs, long totalMs) {
    }

    private final String base;
    private final String token;
    private final HttpClient http;
    private final AtomicLong bytesSent;
    private final java.util.Set<Thread> inFlight = new java.util.HashSet<>();

    public CloudClient(String baseUrl, String token, AtomicLong bytesSent) {
        this.base = baseUrl;
        this.token = token;
        this.bytesSent = bytesSent == null ? new AtomicLong() : bytesSent;
        this.http = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(3))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
    }

    public String baseUrl() {
        return base;
    }

    private HttpRequest.Builder req(String path, long timeoutMs) {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(base + path)).timeout(Duration.ofMillis(timeoutMs));
        if (!token.isEmpty()) {
            b.header("Authorization", "Bearer " + token);
        }
        return b;
    }

    public void abortAll() {
        synchronized (inFlight) {
            for (Thread t : inFlight) {
                t.interrupt();
            }
        }
    }

    private HttpResponse<String> send(HttpRequest r, long bodyBytes) throws IOException {
        return send(r, bodyBytes, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }

    private HttpResponse<String> send(HttpRequest r, long bodyBytes, HttpResponse.BodyHandler<String> handler) throws IOException {
        bytesSent.addAndGet(bodyBytes + HEADER_ESTIMATE);
        Thread self = Thread.currentThread();
        synchronized (inFlight) {
            inFlight.add(self);
        }
        try {
            HttpResponse<String> resp = http.send(r, handler);
            if (resp.statusCode() / 100 != 2) {
                throw new CloudException("HTTP " + resp.statusCode() + " from " + r.uri().getPath() + ": " + abbreviate(resp.body()));
            }
            return resp;
        } catch (InterruptedException e) {
            throw new IOException("request aborted after connectivity change");
        } finally {
            synchronized (inFlight) {
                inFlight.remove(self);
            }
            Thread.interrupted();
        }
    }

    private static String abbreviate(String s) {
        return s == null ? "" : s.length() > 200 ? s.substring(0, 200) : s;
    }

    public boolean health(long timeoutMs) {
        try {
            HttpRequest r = req("/health", timeoutMs).GET().build();
            send(r, 0);
            return true;
        } catch (IOException | RuntimeException e) {
            return false;
        }
    }

    public JsonNode ingest(byte[] gzipBatch, long timeoutMs) throws IOException {
        HttpRequest r = req("/api/v1/ingest", timeoutMs)
                .header("Content-Type", "application/octet-stream")
                .POST(HttpRequest.BodyPublishers.ofByteArray(gzipBatch)).build();
        return Json.read(send(r, gzipBatch.length).body());
    }

    public JsonNode ingest(byte[] gzipBatch, long timeoutMs, Consumer<Timing> timing) throws IOException {
        long start = Clocks.monoMs();
        AtomicLong bodyAt = new AtomicLong(start);
        AtomicLong headersAt = new AtomicLong(start);
        AtomicInteger attempts = new AtomicInteger();
        HttpRequest.BodyPublisher bytes = HttpRequest.BodyPublishers.ofByteArray(gzipBatch);
        HttpRequest.BodyPublisher publisher = new HttpRequest.BodyPublisher() {
            @Override
            public long contentLength() {
                return bytes.contentLength();
            }

            @Override
            public void subscribe(Flow.Subscriber<? super ByteBuffer> subscriber) {
                attempts.incrementAndGet();
                bodyAt.set(Clocks.monoMs());
                bytes.subscribe(subscriber);
            }
        };
        HttpRequest r = req("/api/v1/ingest", timeoutMs)
                .header("Content-Type", "application/octet-stream")
                .POST(publisher).build();
        HttpResponse<String> resp = send(r, gzipBatch.length, info -> {
            headersAt.set(Clocks.monoMs());
            return HttpResponse.BodySubscribers.ofString(StandardCharsets.UTF_8);
        });
        long end = Clocks.monoMs();
        timing.accept(new Timing(gzipBatch.length, attempts.get(), bodyAt.get() - start, headersAt.get() - start, end - start));
        return Json.read(resp.body());
    }

    public Ranges ranges(String session, long timeoutMs) throws IOException {
        HttpRequest r = req("/api/v1/sessions/" + session + "/ranges", timeoutMs).GET().build();
        JsonNode n = Json.read(send(r, 0).body());
        RangeSet[] sets = {null, new RangeSet(), new RangeSet()};
        long[] counts = new long[3];
        for (int s = 1; s <= 2; s++) {
            JsonNode e = n.get("sensor_" + s);
            counts[s] = e.get("held").asLong();
            for (JsonNode pair : e.get("ranges")) {
                sets[s].add(pair.get(0).asLong(), pair.get(1).asLong());
            }
        }
        return new Ranges(sets, counts);
    }

    public void postLosses(String session, List<LossRange> losses, long timeoutMs) throws IOException {
        List<Map<String, Object>> arr = new ArrayList<>();
        for (LossRange l : losses) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("sensor", l.sensor());
            m.put("from_index", l.fromIndex());
            m.put("to_index", l.toIndex());
            m.put("reason", l.reason());
            arr.add(m);
        }
        String body = Json.write(arr);
        send(req("/api/v1/sessions/" + session + "/losses", timeoutMs).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), body.length());
    }

    public void postMeta(String session, boolean closed, long next1, long next2, String reason, long timeoutMs) throws IOException {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("closed", closed);
        m.put("final_next_1", next1);
        m.put("final_next_2", next2);
        m.put("close_reason", reason);
        String body = Json.write(m);
        send(req("/api/v1/sessions/" + session + "/meta", timeoutMs).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), body.length());
    }

    public void postResults(String session, List<String> lines, long timeoutMs) throws IOException {
        String body = String.join("\n", lines) + "\n";
        send(req("/api/v1/sessions/" + session + "/results", timeoutMs).header("Content-Type", "application/x-ndjson")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), body.length());
    }

    public void streamLines(String path, long timeoutMs, Consumer<String> consumer) throws IOException {
        HttpRequest r = req(path, timeoutMs).GET().build();
        bytesSent.addAndGet(HEADER_ESTIMATE);
        try {
            HttpResponse<InputStream> resp = http.send(r, HttpResponse.BodyHandlers.ofInputStream());
            try (InputStream in = resp.body();
                 BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8), 1 << 16)) {
                if (resp.statusCode() / 100 != 2) {
                    throw new CloudException("HTTP " + resp.statusCode() + " from " + path);
                }
                String line;
                while ((line = br.readLine()) != null) {
                    if (!line.isBlank()) {
                        consumer.accept(line);
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted");
        }
    }

    static String lossLine(LossRange l) {
        return ExportFormat.lossLine(l);
    }
}
