package com.mayacardio.pipeline.device.net;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CloudClientTimingTest {
    private HttpServer server;
    private final AtomicInteger requests = new AtomicInteger();
    private final List<byte[]> bodies = new ArrayList<>();
    private volatile long holdFirstMs;

    @BeforeEach
    void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/v1/ingest", ex -> {
            byte[] body;
            try (InputStream in = ex.getRequestBody()) {
                body = in.readAllBytes();
            }
            synchronized (bodies) {
                bodies.add(body);
            }
            if (requests.incrementAndGet() == 1 && holdFirstMs > 0) {
                try {
                    Thread.sleep(holdFirstMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            byte[] out = "{\"held_1\": 7, \"held_2\": 9}".getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(200, out.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(out);
            }
        });
        server.start();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    private CloudClient client() {
        return new CloudClient("http://127.0.0.1:" + server.getAddress().getPort(), "", new AtomicLong());
    }

    @Test
    void slowAcknowledgementIsTimedWithoutChangingTheRequestOrReply() throws IOException {
        holdFirstMs = 2300;
        CloudClient c = client();
        byte[] payload = new byte[5000];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i * 31);
        }
        List<CloudClient.Timing> timings = new ArrayList<>();
        JsonNode slow = c.ingest(payload, 20_000, timings::add);
        JsonNode fast = c.ingest(payload, 20_000, timings::add);

        assertEquals(7, slow.path("held_1").asLong());
        assertEquals(9, fast.path("held_2").asLong());
        assertEquals(2, requests.get());
        assertArrayEquals(payload, bodies.get(0));
        assertArrayEquals(payload, bodies.get(1));

        CloudClient.Timing t = timings.get(0);
        assertEquals(5000, t.bytes());
        assertEquals(1, t.attempts());
        assertTrue(t.connectMs() < 1000, "connect " + t.connectMs());
        assertTrue(t.firstByteMs() >= 2300, "first byte " + t.firstByteMs());
        assertTrue(t.totalMs() >= t.firstByteMs() && t.totalMs() < 5000, "total " + t.totalMs());

        CloudClient.Timing f = timings.get(1);
        assertTrue(f.totalMs() < 2000, "fast total " + f.totalMs());
        assertTrue(f.connectMs() <= f.firstByteMs() && f.firstByteMs() <= f.totalMs());
    }
}
