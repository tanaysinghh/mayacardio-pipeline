package com.mayacardio.pipeline.mock;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class MockServer implements AutoCloseable {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Generator generator;
    private final double speed;
    private final long durationSeconds;
    private final int bufferSeconds;
    private final Path groundTruthDir;
    private final ServerSocket server;
    private final ServerSocket control;
    private volatile MockSession session;
    private volatile Socket client;
    private volatile boolean running = true;
    private final AtomicLong stallUntil = new AtomicLong(Long.MIN_VALUE);
    private final AtomicInteger corruptRemaining = new AtomicInteger();
    private final AtomicInteger duplicateRemaining = new AtomicInteger();
    private final AtomicInteger corruptVariant = new AtomicInteger();
    private final AtomicLong repliesSent = new AtomicLong();

    public MockServer(Generator generator, double speed, long durationSeconds, int bufferSeconds,
                      int port, int controlPort, Path groundTruthDir) throws IOException {
        this.generator = generator;
        this.speed = speed;
        this.durationSeconds = durationSeconds;
        this.bufferSeconds = bufferSeconds;
        this.groundTruthDir = groundTruthDir;
        this.server = new ServerSocket(port, 50, InetAddress.getByName("0.0.0.0"));
        this.control = controlPort >= 0 ? new ServerSocket(controlPort, 50, InetAddress.getLoopbackAddress()) : null;
        this.session = new MockSession(generator, speed, durationSeconds, bufferSeconds);
        if (groundTruthDir != null) {
            Files.createDirectories(groundTruthDir);
        }
    }

    public int port() {
        return server.getLocalPort();
    }

    public int controlPort() {
        return control == null ? -1 : control.getLocalPort();
    }

    public MockSession session() {
        return session;
    }

    public void start() {
        Thread t = new Thread(this::acceptLoop, "mock-accept");
        t.setDaemon(true);
        t.start();
        if (control != null) {
            Thread c = new Thread(this::controlLoop, "mock-control");
            c.setDaemon(true);
            c.start();
        }
        if (groundTruthDir != null) {
            Thread g = new Thread(this::groundTruthLoop, "mock-gt");
            g.setDaemon(true);
            g.start();
        }
    }

    private void acceptLoop() {
        while (running) {
            try (Socket s = server.accept()) {
                client = s;
                s.setTcpNoDelay(true);
                serve(s);
            } catch (IOException e) {
                if (!running) {
                    return;
                }
            } finally {
                client = null;
            }
        }
    }

    private void serve(Socket s) throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
        OutputStream out = s.getOutputStream();
        String line;
        while ((line = in.readLine()) != null) {
            if (System.nanoTime() < stallUntil.get()) {
                continue;
            }
            String reply = handle(line);
            reply = maybeCorrupt(reply);
            byte[] bytes = (reply + "\n").getBytes(StandardCharsets.UTF_8);
            out.write(bytes);
            if (duplicateRemaining.getAndUpdate(v -> v > 0 ? v - 1 : 0) > 0) {
                out.write(bytes);
            }
            out.flush();
            repliesSent.incrementAndGet();
        }
    }

    String handle(String line) {
        try {
            JsonNode req = MAPPER.readTree(line);
            if (!"read".equals(req.path("cmd").asText())) {
                return "{\"error\": \"unknown cmd\"}";
            }
            int sensor = req.path("sensor").asInt(-1);
            if (sensor != 1 && sensor != 2) {
                return "{\"error\": \"unknown sensor\"}";
            }
            Long from = req.has("from_index") && !req.get("from_index").isNull() ? req.get("from_index").asLong() : null;
            return session.read(sensor, from);
        } catch (IOException e) {
            return "{\"error\": \"bad request\"}";
        }
    }

    private String maybeCorrupt(String reply) {
        if (corruptRemaining.getAndUpdate(v -> v > 0 ? v - 1 : 0) <= 0) {
            return reply;
        }
        if (corruptVariant.getAndIncrement() % 2 == 0) {
            int k = reply.indexOf("\"crc32\": \"") + 10;
            char ch = reply.charAt(k);
            char repl = ch == '0' ? '1' : '0';
            return reply.substring(0, k) + repl + reply.substring(k + 1);
        }
        int dataStart = reply.indexOf("\"data\": [") + 9;
        for (int k = dataStart; k < reply.length(); k++) {
            char ch = reply.charAt(k);
            if (ch >= '0' && ch <= '9') {
                char repl = ch == '9' ? '8' : (char) (ch + 1);
                return reply.substring(0, k) + repl + reply.substring(k + 1);
            }
        }
        int k = reply.indexOf("\"crc32\": \"") + 10;
        return reply.substring(0, k) + (reply.charAt(k) == '0' ? '1' : '0') + reply.substring(k + 1);
    }

    public synchronized String restart() {
        MockSession old = session;
        writeGroundTruth(old, true);
        session = new MockSession(generator, speed, durationSeconds, bufferSeconds);
        Socket c = client;
        if (c != null) {
            try {
                c.close();
            } catch (IOException ignored) {
            }
        }
        writeGroundTruth(session, false);
        return session.id();
    }

    public String command(String cmdLine) {
        String[] p = cmdLine.trim().split("\\s+");
        String cmd = p[0].toLowerCase(Locale.ROOT);
        switch (cmd) {
            case "stall" -> {
                double secs = Double.parseDouble(p[1]);
                stallUntil.set(System.nanoTime() + (long) (secs * 1e9));
                return "{\"ok\": true, \"stall_seconds\": " + secs + "}";
            }
            case "corrupt" -> {
                corruptRemaining.addAndGet(Integer.parseInt(p[1]));
                return "{\"ok\": true}";
            }
            case "duplicate" -> {
                duplicateRemaining.addAndGet(Integer.parseInt(p[1]));
                return "{\"ok\": true}";
            }
            case "outage" -> {
                int sensor = Integer.parseInt(p[1]);
                session.startOutage(sensor, Double.parseDouble(p[2]));
                return "{\"ok\": true}";
            }
            case "restart" -> {
                return "{\"ok\": true, \"session_id\": \"" + restart() + "\"}";
            }
            case "status" -> {
                return statusJson(session, false);
            }
            default -> {
                return "{\"ok\": false, \"error\": \"unknown command\"}";
            }
        }
    }

    private void controlLoop() {
        while (running) {
            try (Socket s = control.accept()) {
                BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
                PrintWriter out = new PrintWriter(s.getOutputStream(), true, StandardCharsets.UTF_8);
                String line;
                while ((line = in.readLine()) != null) {
                    if (line.isBlank()) {
                        continue;
                    }
                    String reply;
                    try {
                        reply = command(line);
                    } catch (RuntimeException e) {
                        reply = "{\"ok\": false, \"error\": \"" + e.getClass().getSimpleName() + "\"}";
                    }
                    out.println(reply);
                }
            } catch (SocketException e) {
                if (!running) {
                    return;
                }
            } catch (IOException e) {
                if (!running) {
                    return;
                }
            }
        }
    }

    private void groundTruthLoop() {
        while (running) {
            writeGroundTruth(session, false);
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    static String statusJson(MockSession s, boolean ended) {
        StringBuilder o = new StringBuilder();
        for (int sensor = 1; sensor <= 2; sensor++) {
            for (long[] r : s.outages(sensor)) {
                if (o.length() > 0) {
                    o.append(", ");
                }
                o.append("{\"sensor\": ").append(sensor).append(", \"from_index\": ").append(r[0])
                        .append(", \"to_index\": ").append(r[1]).append("}");
            }
        }
        long head = s.head();
        return "{\"session_id\": \"" + s.id() + "\", \"mode\": \"" + s.generator().mode() + "\", \"seed\": "
                + s.generator().seed() + ", \"speed\": " + s.speed() + ", \"started_at_ms\": " + s.startedAtMillis()
                + ", \"generated\": " + head + ", \"total\": " + (s.totalSamples() == Long.MAX_VALUE ? -1 : s.totalSamples())
                + ", \"ended\": " + (ended || head >= s.totalSamples()) + ", \"outages\": [" + o + "]}";
    }

    private synchronized void writeGroundTruth(MockSession s, boolean ended) {
        if (groundTruthDir == null) {
            return;
        }
        try {
            Path target = groundTruthDir.resolve(s.id() + ".json");
            Path tmp = groundTruthDir.resolve(s.id() + ".json.tmp");
            Files.writeString(tmp, statusJson(s, ended) + "\n");
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException ignored) {
        }
    }

    public long repliesSent() {
        return repliesSent.get();
    }

    public List<String> describe() {
        return List.of("port=" + port(), "control=" + controlPort(), "session=" + session.id());
    }

    @Override
    public void close() throws IOException {
        running = false;
        writeGroundTruth(session, false);
        server.close();
        if (control != null) {
            control.close();
        }
        Socket c = client;
        if (c != null) {
            c.close();
        }
    }
}
