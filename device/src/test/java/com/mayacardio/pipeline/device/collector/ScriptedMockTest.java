package com.mayacardio.pipeline.device.collector;

import com.fasterxml.jackson.databind.JsonNode;
import com.mayacardio.pipeline.core.Json;
import com.mayacardio.pipeline.core.LossRange;
import com.mayacardio.pipeline.core.SampleBlock;
import com.mayacardio.pipeline.device.DeviceState;
import com.mayacardio.pipeline.device.store.DeviceStore;
import com.mayacardio.pipeline.mock.Generator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ScriptedMockTest {
    private static final Generator RAMP = new Generator("ramp", 7);

    @TempDir
    Path dir;
    private ServerSocket server;
    private Thread serverThread;

    @AfterEach
    void stop() throws Exception {
        if (server != null) {
            server.close();
        }
    }

    private void serve(Function<JsonNode, List<String>> handler) throws Exception {
        server = new ServerSocket(0);
        serverThread = new Thread(() -> {
            while (!server.isClosed()) {
                try (Socket s = server.accept()) {
                    BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
                    OutputStream out = s.getOutputStream();
                    String line;
                    while ((line = in.readLine()) != null) {
                        List<String> replies;
                        synchronized (this) {
                            replies = handler.apply(Json.read(line));
                        }
                        for (String r : replies) {
                            out.write((r + "\n").getBytes(StandardCharsets.UTF_8));
                        }
                        out.flush();
                    }
                } catch (Exception ignored) {
                }
            }
        });
        serverThread.setDaemon(true);
        serverThread.start();
    }

    static String reply(String session, int sensor, long first, int count, String lostJson, boolean end) {
        StringBuilder d = new StringBuilder("[");
        for (int k = 0; k < count; k++) {
            if (k > 0) {
                d.append(", ");
            }
            d.append(Arrays.toString(RAMP.sample(sensor, first + k)));
        }
        String data = d.append(']').toString();
        CRC32 crc = new CRC32();
        crc.update(data.getBytes(StandardCharsets.UTF_8));
        return "{\"session_id\": \"" + session + "\", \"sensor\": " + sensor + ", \"first_index\": " + first + ", \"count\": " + count
                + ", \"lost\": [" + lostJson + "], \"session_end\": " + end + ", \"crc32\": \"" + String.format("%08x", crc.getValue())
                + "\", \"data\": " + data + "}";
    }

    private DeviceState run(long millis) throws Exception {
        DeviceState state = new DeviceState();
        try (DeviceStore store = DeviceStore.open(dir.resolve("pipeline.db"))) {
            DiskGuard guard = new DiskGuard(dir, 0, 0, 0, d -> Long.MAX_VALUE);
            Collector c = new Collector(store, state, new MockClient("127.0.0.1", server.getLocalPort(), 2000), guard, 50, 48_000);
            Thread t = new Thread(c);
            t.start();
            Thread.sleep(millis);
            c.stop();
            t.join(10_000);
        }
        return state;
    }

    record Check(long[] next, List<LossRange> losses) {
    }

    private Check verify(String session) throws Exception {
        try (DeviceStore store = DeviceStore.open(dir.resolve("pipeline.db"))) {
            DeviceStore.SessionInfo info = store.session(session);
            assertNotNull(info, "session " + session);
            List<LossRange> losses = store.losses(info.sid());
            long[] next = new long[3];
            for (int s = 1; s <= 2; s++) {
                next[s] = store.stream(info.sid(), s).nextIndex();
                SampleBlock b = store.block(info.sid(), s, 0, Math.max(0, next[s] - 1));
                for (long i = 0; i < next[s]; i++) {
                    boolean lost = false;
                    for (LossRange l : losses) {
                        lost |= l.sensor() == s && i >= l.fromIndex() && i <= l.endInclusive();
                    }
                    assertTrue(b.has(i) ^ lost, "sensor " + s + " index " + i + " stored=" + b.has(i) + " lost=" + lost);
                    if (b.has(i)) {
                        assertTrue(Arrays.equals(RAMP.sample(s, i), b.row(i)), "value at " + s + "/" + i);
                    }
                }
            }
            return new Check(next, losses);
        }
    }

    private static long from(JsonNode req) {
        return req.has("from_index") ? req.get("from_index").asLong() : -1;
    }

    @Test
    void lostReportedOnceAfterDataKeepsTheMockReason() throws Exception {
        serve(req -> {
            int s = req.get("sensor").asInt();
            long f = from(req);
            if (f < 0) {
                return List.of(reply("s1", s, 0, 0, "", false));
            }
            if (s == 2) {
                return List.of(reply("s1", 2, f, (int) Math.max(0, Math.min(400, 2000 - f)), "", false));
            }
            if (f == 0) {
                return List.of(reply("s1", 1, 0, 400, "{\"from_index\": 400, \"to_index\": 799, \"reason\": \"sensor_outage\"}", false));
            }
            if (f >= 400 && f < 800) {
                return List.of(reply("s1", 1, 800, 0, "", false));
            }
            return List.of(reply("s1", 1, f, (int) Math.max(0, Math.min(400, 2000 - f)), "", false));
        });
        run(1500);
        Check c = verify("s1");
        assertEquals(2000, c.next()[1]);
        assertEquals(List.of(new LossRange(1, 400, 799L, "sensor_outage")), c.losses());
    }

    @Test
    void unevenAndOversizedRepliesLoseNothing() throws Exception {
        long[] served = new long[3];
        serve(req -> {
            int s = req.get("sensor").asInt();
            long f = Math.max(0, from(req));
            int n = (int) Math.min((s == 1 ? 1000 : 37) + served[s] % 5, Math.max(0, 6000 - f));
            served[s]++;
            return List.of(reply("s2", s, f, n, "", false));
        });
        DeviceState st = run(2500);
        assertEquals(0, st.timeouts.get());
        Check c = verify("s2");
        assertEquals(6000, c.next()[1]);
        assertEquals(6000, c.next()[2]);
        assertTrue(c.losses().isEmpty(), c.losses().toString());
    }

    @Test
    void emptyReplyThatSkipsAheadIsLabelledNotDelivered() throws Exception {
        serve(req -> {
            int s = req.get("sensor").asInt();
            long f = Math.max(0, from(req));
            if (s == 2 && f >= 400 && f < 1000) {
                return List.of(reply("s3", 2, 1000, 0, "", false));
            }
            return List.of(reply("s3", s, f, (int) Math.max(0, Math.min(400, 2000 - f)), "", false));
        });
        run(13_000);
        Check c = verify("s3");
        assertEquals(2000, c.next()[2]);
        assertEquals(List.of(new LossRange(2, 400, 999L, Collector.R_UNDELIVERED)), c.losses());
    }

    @Test
    void silentRestartAnsweringFromIndexWithErrorsIsDetected() throws Exception {
        boolean[] restarted = {false};
        long[] reached = new long[3];
        serve(req -> {
            int s = req.get("sensor").asInt();
            long f = from(req);
            if (!restarted[0]) {
                long start = Math.max(0, f);
                int n = (int) Math.max(0, Math.min(400, 2000 - start));
                reached[s] = Math.max(reached[s], start + n);
                if (reached[1] >= 2000 && reached[2] >= 2000) {
                    restarted[0] = true;
                }
                return List.of(reply("old1", s, start, n, "", false));
            }
            if (f >= 1200) {
                return List.of("{\"error\": \"from_index out of range\"}");
            }
            long start = Math.max(0, f);
            return List.of(reply("new1", s, start, (int) Math.max(0, Math.min(400, 1200 - start)), "", false));
        });
        run(3000);
        Check old = verify("old1");
        assertEquals(2, old.losses().size());
        for (LossRange l : old.losses()) {
            assertEquals(2000, l.fromIndex());
            assertEquals(null, l.toIndex());
            assertEquals(Collector.R_SESSION_RESTART, l.reason());
        }
        Check fresh = verify("new1");
        assertEquals(1200, fresh.next()[1]);
        assertTrue(fresh.losses().isEmpty(), fresh.losses().toString());
    }

    @Test
    void fallsBackToPlainReadsWhenFromIndexKeepsFailing() throws Exception {
        long[] cursor = new long[3];
        serve(req -> {
            int s = req.get("sensor").asInt();
            if (req.has("from_index")) {
                return List.of("{\"error\": \"from_index not supported\"}");
            }
            long f = cursor[s];
            int n = (int) Math.max(0, Math.min(400, 4000 - f));
            cursor[s] = f + n;
            return List.of(reply("p1", s, f, n, "", false));
        });
        DeviceState st = run(3000);
        Check c = verify("p1");
        assertEquals(4000, c.next()[1]);
        assertEquals(4000, c.next()[2]);
        long lost = 0;
        for (LossRange l : c.losses()) {
            assertEquals(Collector.R_UNDELIVERED, l.reason());
            lost += l.size();
        }
        assertTrue(lost <= 800, "lost " + lost);
        assertTrue(st.corruptReplies.get() >= 3);
    }

    @Test
    void lostRangeOverlappingDataKeepsTheData() throws Exception {
        serve(req -> {
            int s = req.get("sensor").asInt();
            long f = Math.max(0, from(req));
            if (s == 1 && f == 0) {
                return List.of(reply("o1", 1, 300, 400, "{\"from_index\": 0, \"to_index\": 499, \"reason\": \"buffer_overflow\"}", false));
            }
            return List.of(reply("o1", s, f, (int) Math.max(0, Math.min(400, 2000 - f)), "", false));
        });
        run(1500);
        Check c = verify("o1");
        assertEquals(2000, c.next()[1]);
        assertEquals(List.of(new LossRange(1, 0, 299L, "buffer_overflow")), c.losses());
    }
}
