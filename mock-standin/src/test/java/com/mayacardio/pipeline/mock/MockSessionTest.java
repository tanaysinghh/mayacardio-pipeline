package com.mayacardio.pipeline.mock;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MockSessionTest {
    private static final ObjectMapper M = new ObjectMapper();

    private static String dataText(String reply) {
        int k = reply.indexOf("\"data\": ") + 8;
        return reply.substring(k, reply.length() - 1);
    }

    @Test
    void rampValuesFollowSpec() {
        Generator g = new Generator("ramp", 7);
        assertArrayEquals(new int[]{1, 2, 3}, g.sample(1, 0));
        assertArrayEquals(new int[]{(int) ((3L * 5000000 + 1) % 8388608), (int) ((3L * 5000000 + 2) % 8388608),
                (int) ((3L * 5000000 + 3) % 8388608)}, g.sample(1, 5000000));
        assertArrayEquals(new int[]{2 * 10 + 1, 2 * 10 + 2}, g.sample(2, 10));
    }

    @Test
    void signalIsDeterministicAndScaled() {
        Generator a = new Generator("signal", 7);
        Generator b = new Generator("signal", 7);
        Generator c = new Generator("signal", 8);
        for (long i = 0; i < 2000; i += 37) {
            assertArrayEquals(a.sample(1, i), b.sample(1, i));
            assertArrayEquals(a.sample(2, i), b.sample(2, i));
        }
        int[] s = a.sample(1, 120);
        assertEquals(Math.round(0.6 * s[1]), s[0]);
        assertEquals(Math.round(1.3 * s[1]), s[2]);
        assertNotEquals(a.sample(2, 5)[0], c.sample(2, 5)[0] + 1_000_000);
        int[] s2 = a.sample(2, 1000);
        assertTrue(Math.abs(s2[0] - 400000) < 30000);
        assertTrue(Math.abs(s2[1] - 150000) < 10000);
    }

    @Test
    void signalHasOnePeakPerBeatAtScheduledRates() {
        Generator g = new Generator("signal", 7);
        assertEquals(60, Generator.bpmAt(30));
        assertEquals(90, Generator.bpmAt(150));
        assertEquals(72, Generator.bpmAt(250));
        assertEquals(60, Generator.bpmAt(370));
        int peaks = 0;
        for (long i = 801; i < 400 * 60; i++) {
            int v = g.sample(1, i)[1];
            if (v > g.sample(1, i - 1)[1] && v > g.sample(1, i + 1)[1] && v > 700_000) {
                peaks++;
            }
        }
        assertTrue(Math.abs(peaks - 58) <= 1, "peaks=" + peaks);
    }

    @Test
    void replyCrcCoversDataTextAndRespectsLimit() throws Exception {
        MockSession s = new MockSession(new Generator("ramp", 7), 100.0, 0, 10);
        Thread.sleep(50);
        String reply = s.read(1, 0L);
        JsonNode n = M.readTree(reply);
        assertEquals(0, n.get("first_index").asLong());
        assertTrue(n.get("count").asInt() <= 400);
        assertEquals(n.get("count").asInt(), n.get("data").size());
        assertEquals(MockSession.crcHex(dataText(reply)), n.get("crc32").asText());
        assertEquals("[1, 2, 3]", n.get("data").get(0).toString().replace(",", ", "));
    }

    @Test
    void bufferOverflowIsReportedAsLost() throws Exception {
        MockSession s = new MockSession(new Generator("ramp", 7), 400.0, 0, 10);
        Thread.sleep(200);
        long head = s.head();
        assertTrue(head > 4000 + 1000);
        JsonNode n = M.readTree(s.read(2, null));
        JsonNode lost = n.get("lost").get(0);
        assertEquals(0, lost.get("from_index").asLong());
        assertEquals("buffer_overflow", lost.get("reason").asText());
        assertEquals(lost.get("to_index").asLong() + 1, n.get("first_index").asLong());
    }

    @Test
    void outageIsReportedAndOtherSensorUnaffected() throws Exception {
        MockSession s = new MockSession(new Generator("ramp", 7), 50.0, 0, 10);
        Thread.sleep(40);
        long from = s.head();
        s.startOutage(2, 0.5);
        Thread.sleep(100);
        long next = 0;
        boolean sawOutage = false;
        for (int k = 0; k < 40; k++) {
            JsonNode n = M.readTree(s.read(2, next));
            for (JsonNode l : n.get("lost")) {
                assertEquals("sensor_outage", l.get("reason").asText());
                assertTrue(l.get("from_index").asLong() >= from);
                sawOutage = true;
            }
            next = n.get("first_index").asLong() + n.get("count").asLong();
        }
        assertTrue(sawOutage);
        JsonNode one = M.readTree(s.read(1, from));
        assertEquals(0, one.get("lost").size());
        assertEquals(from, one.get("first_index").asLong());
    }

    @Test
    void sessionEndFlagOnLastReply() throws Exception {
        MockSession s = new MockSession(new Generator("ramp", 7), 400.0, 2, 10);
        Thread.sleep(50);
        JsonNode n = M.readTree(s.read(1, 400L));
        assertEquals(400, n.get("count").asInt());
        assertTrue(n.get("session_end").asBoolean());
        assertEquals(400, n.get("first_index").asLong());
    }

    @Test
    void serverFaultsAndRestartOverTcp() throws Exception {
        try (MockServer server = new MockServer(new Generator("ramp", 7), 10.0, 0, 10, 0, 0, null)) {
            server.start();
            try (Socket sock = new Socket("127.0.0.1", server.port())) {
                BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream(), StandardCharsets.UTF_8));
                PrintWriter out = new PrintWriter(sock.getOutputStream(), true, StandardCharsets.UTF_8);
                Thread.sleep(100);
                server.command("corrupt 1");
                out.println("{\"cmd\": \"read\", \"sensor\": 1, \"from_index\": 0}");
                String bad = in.readLine();
                JsonNode badNode = M.readTree(bad);
                assertNotEquals(MockSession.crcHex(dataText(bad)), badNode.get("crc32").asText());
                server.command("duplicate 1");
                out.println("{\"cmd\": \"read\", \"sensor\": 1, \"from_index\": 0}");
                String a = in.readLine();
                String b = in.readLine();
                assertEquals(a, b);
                String oldId = M.readTree(a).get("session_id").asText();
                String newId = M.readTree(server.command("restart")).get("session_id").asText();
                assertNotEquals(oldId, newId);
                assertEquals(null, in.readLine());
            }
        }
    }

    @Test
    void muteDropsRequestsForOneSensorOnly() throws Exception {
        try (MockServer server = new MockServer(new Generator("ramp", 7), 10.0, 0, 10, 0, -1, null)) {
            server.start();
            try (Socket sock = new Socket("127.0.0.1", server.port())) {
                sock.setSoTimeout(500);
                BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream(), StandardCharsets.UTF_8));
                PrintWriter out = new PrintWriter(sock.getOutputStream(), true, StandardCharsets.UTF_8);
                server.command("mute 2 1");
                out.println("{\"cmd\": \"read\", \"sensor\": 2, \"from_index\": 0}");
                out.println("{\"cmd\": \"read\", \"sensor\": 1, \"from_index\": 0}");
                assertEquals(1, M.readTree(in.readLine()).get("sensor").asInt());
                Thread.sleep(1100);
                out.println("{\"cmd\": \"read\", \"sensor\": 2, \"from_index\": 0}");
                assertEquals(2, M.readTree(in.readLine()).get("sensor").asInt());
            }
        }
    }
}
