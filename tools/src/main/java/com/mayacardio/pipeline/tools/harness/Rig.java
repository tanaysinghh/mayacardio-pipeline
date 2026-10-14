package com.mayacardio.pipeline.tools.harness;

import com.fasterxml.jackson.databind.JsonNode;
import com.mayacardio.pipeline.core.Json;
import com.mayacardio.pipeline.tools.CheckResults;
import com.mayacardio.pipeline.tools.CheckStandin;
import com.mayacardio.pipeline.tools.CompareExports;
import com.mayacardio.pipeline.tools.ExportData;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public final class Rig implements AutoCloseable {
    public static final String TOKEN = "harness-token";

    final Path work;
    final Path jars;
    final String java;
    final double speed;
    final String mode;
    final long startMono = System.nanoTime();
    final List<Map<String, Object>> timeline = new ArrayList<>();
    final Map<String, String> deviceExtra = new LinkedHashMap<>();
    final int mockPort;
    final int mockCtl;
    final int cloudPort;
    Path dataDir;
    Path runDir;
    final Path gtDir;
    final Path conf;
    NetProxy proxy;
    Proc mock;
    Proc cloud;
    Proc device;
    int mockStarts;
    int deviceStarts;

    public Rig(Path work, Path jars, double speed, String mode) throws IOException {
        this.work = work;
        this.jars = jars;
        this.speed = speed;
        this.mode = mode;
        this.java = ProcessHandle.current().info().command().orElse("java");
        Files.createDirectories(work);
        this.mockPort = freePort();
        this.mockCtl = freePort();
        this.cloudPort = freePort();
        this.dataDir = work.resolve("device-data");
        this.runDir = work.resolve("device-run");
        this.gtDir = work.resolve("ground-truth");
        this.conf = work.resolve("device.conf");
    }

    static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    public long tMs() {
        return (System.nanoTime() - startMono) / 1_000_000;
    }

    public void event(String what, Object detail) {
        Map<String, Object> e = new LinkedHashMap<>();
        e.put("t_ms", tMs());
        e.put("wall", Instant.now().toString());
        e.put("event", what);
        if (detail != null) {
            e.put("detail", detail);
        }
        timeline.add(e);
        System.out.println("[" + e.get("t_ms") + " ms] " + what + (detail == null ? "" : " " + detail));
    }

    public void startCloud() throws Exception {
        Map<String, String> env = Map.of("CLOUD_SQLITE_PATH", work.resolve("cloud-data/cloud.db").toString(),
                "CLOUD_API_TOKEN", TOKEN);
        cloud = new Proc("cloud", List.of(java, "-Xmx512m", "-jar", jars.resolve("cloud-1.0.0-boot.jar").toString(),
                "--server.port=" + cloudPort), work.resolve("cloud.log"), env).start();
        long deadline = System.nanoTime() + 60_000_000_000L;
        while (System.nanoTime() < deadline) {
            if (httpOk("http://127.0.0.1:" + cloudPort + "/health")) {
                break;
            }
            Thread.sleep(250);
        }
        proxy = new NetProxy("127.0.0.1", cloudPort);
        event("cloud_started", "port " + cloudPort + ", proxy " + proxy.port());
    }

    static boolean httpOk(String url) {
        try {
            HttpURLConnection c = (HttpURLConnection) URI.create(url).toURL().openConnection();
            c.setConnectTimeout(500);
            c.setReadTimeout(1000);
            return c.getResponseCode() == 200;
        } catch (IOException e) {
            return false;
        }
    }

    public void startMock() throws Exception {
        mock = new Proc("mock", List.of(java, "-Xmx256m", "-jar", jars.resolve("biosignal-mock-standin.jar").toString(),
                "--mode", mode, "--seed", "7", "--duration", "0", "--port", Integer.toString(mockPort),
                "--control-port", Integer.toString(mockCtl), "--speed", Double.toString(speed),
                "--ground-truth-dir", gtDir.toString()), work.resolve("mock.log"), null).start();
        mockStarts++;
        long deadline = System.nanoTime() + 30_000_000_000L;
        while (System.nanoTime() < deadline) {
            try (Socket s = new Socket("127.0.0.1", mockCtl)) {
                break;
            } catch (IOException e) {
                Thread.sleep(100);
            }
        }
        event("mock_started", "pid " + mock.pid());
    }

    public void writeDeviceConf() throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("mock.host=127.0.0.1\n");
        sb.append("mock.port=").append(mockPort).append('\n');
        sb.append("data.dir=").append(dataDir.toString().replace('\\', '/')).append('\n');
        sb.append("run.dir=").append(runDir.toString().replace('\\', '/')).append('\n');
        if (proxy != null) {
            sb.append("cloud.url=http://127.0.0.1:").append(proxy.port()).append('\n');
            sb.append("cloud.token=").append(TOKEN).append('\n');
        }
        for (Map.Entry<String, String> e : deviceExtra.entrySet()) {
            sb.append(e.getKey()).append('=').append(e.getValue()).append('\n');
        }
        Files.writeString(conf, sb.toString());
    }

    public void startDevice() throws Exception {
        writeDeviceConf();
        device = new Proc("device", List.of(java, "-Xmx256m", "-jar", jars.resolve("device-1.0.0-all.jar").toString(),
                "run", "--config", conf.toString()), work.resolve("device.log"), null).start();
        deviceStarts++;
        event("device_started", "pid " + device.pid());
    }

    public void killDevice() throws InterruptedException {
        long pid = device.pid();
        device.kill9();
        event("device_killed_9", "pid " + pid);
    }

    public void killMock() throws InterruptedException {
        long pid = mock.pid();
        mock.kill9();
        event("mock_killed_9", "pid " + pid);
    }

    public String mockCmd(String cmd) throws IOException {
        try (Socket s = new Socket("127.0.0.1", mockCtl);
             PrintWriter out = new PrintWriter(s.getOutputStream(), true, StandardCharsets.UTF_8);
             BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8))) {
            out.println(cmd);
            String reply = in.readLine();
            event("mock_" + cmd.split(" ")[0], cmd + " -> " + reply);
            return reply;
        }
    }

    public void cutNetwork() {
        proxy.cut();
        event("network_cut", null);
    }

    public void restoreNetwork() {
        proxy.restore();
        event("network_restored", null);
    }

    public JsonNode status() {
        Path f = runDir.resolve("status.json");
        for (int i = 0; i < 20; i++) {
            try {
                return Json.read(Files.readString(f));
            } catch (IOException | IllegalArgumentException e) {
                sleep(25);
            }
        }
        return null;
    }

    public long waitStatus(Predicate<JsonNode> p, long timeoutMs) {
        long start = tMs();
        while (tMs() - start < timeoutMs) {
            JsonNode s = status();
            if (s != null && p.test(s)) {
                return tMs() - start;
            }
            sleep(100);
        }
        return -1;
    }

    public long waitMode(String m, long timeoutMs) {
        return waitStatus(s -> m.equals(s.path("mode").asText()), timeoutMs);
    }

    public long inFlightTolerance() {
        return (long) Math.ceil(2 * 2 * 400 * speed);
    }

    public long waitSynced(long timeoutMs) {
        long tol = inFlightTolerance();
        return waitStatus(s -> s.path("backlog").asLong() <= tol && s.path("catchup_backlog_samples").asLong() == 0
                && "online".equals(s.path("mode").asText()), timeoutMs);
    }

    static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void sleepUntil(long tMs) {
        long now = tMs();
        if (tMs > now) {
            sleep(tMs - now);
        }
    }

    public int cli(Path stdout, String... args) throws Exception {
        List<String> cmd = new ArrayList<>(List.of(java, "-jar", jars.resolve("device-1.0.0-all.jar").toString()));
        cmd.addAll(List.of(args));
        cmd.add("--config");
        cmd.add(conf.toString());
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectOutput(stdout.toFile());
        pb.redirectError(ProcessBuilder.Redirect.appendTo(work.resolve("cli-stderr.log").toFile()));
        Process p = pb.start();
        return p.waitFor();
    }

    public List<JsonNode> sessions() throws Exception {
        Path out = work.resolve("sessions.jsonl");
        cli(out, "sessions");
        List<JsonNode> list = new ArrayList<>();
        for (String l : Files.readAllLines(out)) {
            if (!l.isBlank()) {
                list.add(Json.read(l));
            }
        }
        return list;
    }

    public Map<String, Object> verifyAll(boolean currentIsOpen) throws Exception {
        Map<String, Object> out = new LinkedHashMap<>();
        boolean pass = true;
        JsonNode st = status();
        for (JsonNode s : sessions()) {
            String id = s.get("session").asText();
            Map<String, Object> r = new LinkedHashMap<>();
            Path dev = work.resolve("export-device-" + id + ".jsonl");
            Path cl = work.resolve("export-cloud-" + id + ".jsonl");
            Path res = work.resolve("results-" + id + ".jsonl");
            cli(dev, "export", "--session", id, "--from", "device");
            int cloudExit = cli(cl, "export", "--session", id, "--from", "cloud");
            cli(res, "results", "--session", id, "--from", "both");
            ExportData d = ExportData.read(dev);
            Long until = null;
            boolean open = !s.get("closed").asBoolean();
            if (open && currentIsOpen) {
                until = Math.min(s.get("next_index_1").asLong(), s.get("next_index_2").asLong()) - 1;
            }
            Path gt = gtDir.resolve(id + ".json");
            if (Files.exists(gt)) {
                Map<String, Object> c = CheckStandin.check(d, Json.read(Files.readString(gt)), until);
                r.put("ground_truth_check", c);
                pass &= Boolean.TRUE.equals(c.get("pass"));
            } else {
                r.put("ground_truth_check", "missing ground truth file");
                pass = false;
            }
            if (cloudExit == 0) {
                Long cmpUntil = until == null ? null : until - inFlightTolerance();
                Map<String, Object> cmp = CompareExports.compare(d, ExportData.read(cl), cmpUntil);
                r.put("device_vs_cloud", cmp);
                pass &= Boolean.TRUE.equals(cmp.get("identical"));
            } else {
                r.put("device_vs_cloud", "cloud export failed");
                pass = false;
            }
            Map<String, Object> rc = CheckResults.check(d, res);
            r.put("results_check", rc);
            pass &= Boolean.TRUE.equals(rc.get("pass"));
            r.put("results_lines", Files.readAllLines(res).size());
            r.put("device_session_summary", Json.MAPPER.convertValue(s, Map.class));
            out.put(id, r);
        }
        out.put("final_status", st == null ? null : Json.MAPPER.convertValue(st, Map.class));
        out.put("pass", pass);
        return out;
    }

    public Map<String, Object> latency(String sessionId, List<long[]> disruptedMs, long windowsFromMs) throws Exception {
        Path gt = gtDir.resolve(sessionId + ".json");
        JsonNode g = Json.read(Files.readString(gt));
        long started = g.get("started_at_ms").asLong();
        double sp = g.get("speed").asDouble();
        Path res = work.resolve("results-" + sessionId + ".jsonl");
        long rigStartWall = Instant.parse((String) timeline.get(0).get("wall")).toEpochMilli() - (long) timeline.get(0).get("t_ms");
        List<Long> lat = new ArrayList<>();
        long over5 = 0;
        long excluded = 0;
        long maxLat = 0;
        long maxW = -1;
        for (String l : Files.readAllLines(res)) {
            JsonNode n = Json.read(l);
            if (!"online".equals(n.path("mode").asText())) {
                continue;
            }
            long w = n.get("window").asLong();
            long endWall = started + (long) ((w + 1) * 1000.0 / sp);
            long endT = endWall - rigStartWall;
            boolean disturbed = endT < windowsFromMs;
            for (long[] d : disruptedMs) {
                if (endT >= d[0] - 1000 && endT <= d[1] + 10_000) {
                    disturbed = true;
                }
            }
            if (disturbed) {
                excluded++;
                continue;
            }
            long computed = Instant.parse(n.get("computed_at").asText()).toEpochMilli();
            long latency = computed - endWall;
            lat.add(latency);
            if (latency > 5000) {
                over5++;
            }
            if (latency > maxLat) {
                maxLat = latency;
                maxW = w;
            }
        }
        lat.sort(Long::compare);
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("definition", "computed_at (cloud wall clock) minus the wall time at which the window's last sample was generated by the mock; same machine clock");
        m.put("windows_measured", lat.size());
        m.put("windows_excluded_near_disruptions", excluded);
        if (!lat.isEmpty()) {
            m.put("p50_ms", lat.get(lat.size() / 2));
            m.put("p95_ms", lat.get((int) Math.min(lat.size() - 1, Math.round(lat.size() * 0.95))));
            m.put("max_ms", maxLat);
            m.put("max_window", maxW);
        }
        m.put("windows_over_5s", over5);
        return m;
    }

    @Override
    public void close() {
        for (Proc p : new Proc[]{device, mock, cloud}) {
            if (p != null) {
                try {
                    p.terminate();
                } catch (InterruptedException ignored) {
                }
            }
        }
        if (proxy != null) {
            try {
                proxy.close();
            } catch (IOException ignored) {
            }
        }
    }
}
