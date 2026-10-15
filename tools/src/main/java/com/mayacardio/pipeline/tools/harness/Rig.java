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
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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
    int mockPort;
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
    int mockBufferSeconds = 10;
    String externalCloudUrl;
    String mockCmdTemplate;
    final Map<String, String> processEnv = new LinkedHashMap<>();
    String checkerCmdTemplate;
    String externalCloudToken;
    String netCmdTemplate;
    String deviceLauncher;
    long msUntilCloudHealthy = -1;
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
        if (externalCloudUrl != null) {
            URI u = URI.create(externalCloudUrl);
            long t0 = tMs();
            long deadline = System.nanoTime() + 300_000_000_000L;
            while (System.nanoTime() < deadline && !httpOk(externalCloudUrl + "/health")) {
                Thread.sleep(500);
            }
            msUntilCloudHealthy = tMs() - t0;
            if (netCmdTemplate != null) {
                event("external_cloud", externalCloudUrl + " direct, healthy after " + msUntilCloudHealthy + " ms; network cut by command");
                return;
            }
            int port = u.getPort() > 0 ? u.getPort() : 80;
            proxy = new NetProxy(u.getHost(), port);
            event("external_cloud", externalCloudUrl + " via proxy " + proxy.port());
            return;
        }
        Map<String, String> env = Map.of("CLOUD_SQLITE_PATH", work.resolve("cloud-data/cloud.db").toString(),
                "CLOUD_API_TOKEN", TOKEN, "PORT", Integer.toString(cloudPort));
        cloud = new Proc("cloud", List.of(java, "-Xmx512m", "-jar", jars.resolve("cloud-1.0.0-all.jar").toString()),
                work.resolve("cloud.log"), withProcessEnv(env)).start();
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

    public JsonNode cloudStatus(String session) {
        try {
            HttpURLConnection c = (HttpURLConnection) URI.create(cloudBase() + "/api/v1/sessions/" + session + "/status")
                    .toURL().openConnection();
            c.setConnectTimeout(5000);
            c.setReadTimeout(30_000);
            c.setRequestProperty("Authorization", "Bearer " + token());
            if (c.getResponseCode() != 200) {
                return null;
            }
            return Json.read(new String(c.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
        } catch (IOException | RuntimeException e) {
            return null;
        }
    }

    String cloudBase() {
        return proxy != null ? "http://127.0.0.1:" + proxy.port() : externalCloudUrl;
    }

    String token() {
        return externalCloudToken != null ? externalCloudToken : TOKEN;
    }

    static boolean httpOk(String url) {
        try {
            HttpURLConnection c = (HttpURLConnection) URI.create(url).toURL().openConnection();
            c.setConnectTimeout(2000);
            c.setReadTimeout(5000);
            return c.getResponseCode() == 200;
        } catch (IOException e) {
            return false;
        }
    }

    Map<String, String> withProcessEnv(Map<String, String> base) {
        Map<String, String> m = new LinkedHashMap<>();
        if (base != null) {
            m.putAll(base);
        }
        m.putAll(processEnv);
        return m;
    }

    public boolean externalMock() {
        return mockCmdTemplate != null;
    }

    static List<String> shell(String command) {
        boolean windows = System.getProperty("os.name").toLowerCase().contains("win");
        return windows ? List.of("cmd.exe", "/c", command) : List.of("sh", "-c", "exec " + command);
    }

    public void startMock() throws Exception {
        if (externalMock()) {
            Files.createDirectories(gtDir);
            mock = new Proc("mock", shell(mockCmdTemplate.replace("{port}", Integer.toString(mockPort))),
                    work.resolve("mock.log"), withProcessEnv(Map.of("BIOSIGNAL_MOCK_GT_DIR", gtDir.toString()))).start();
            mockStarts++;
            long deadline = System.nanoTime() + 60_000_000_000L;
            while (System.nanoTime() < deadline) {
                try (Socket s = new Socket("127.0.0.1", mockPort)) {
                    break;
                } catch (IOException e) {
                    Thread.sleep(200);
                }
            }
            event("mock_started", "external command, pid " + mock.pid());
            return;
        }
        mock = new Proc("mock", List.of(java, "-Xmx256m", "-jar", jars.resolve("biosignal-mock-standin.jar").toString(),
                "--mode", mode, "--seed", "7", "--duration", "0", "--port", Integer.toString(mockPort),
                "--control-port", Integer.toString(mockCtl), "--speed", Double.toString(speed),
                "--buffer-seconds", Integer.toString(mockBufferSeconds),
                "--ground-truth-dir", gtDir.toString()), work.resolve("mock.log"), withProcessEnv(null)).start();
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
        if (proxy != null || externalCloudUrl != null) {
            sb.append("cloud.url=").append(cloudBase()).append('\n');
            sb.append("cloud.token=").append(token()).append('\n');
        }
        for (Map.Entry<String, String> e : deviceExtra.entrySet()) {
            sb.append(e.getKey()).append('=').append(e.getValue()).append('\n');
        }
        Files.writeString(conf, sb.toString());
    }

    public void startDevice() throws Exception {
        writeDeviceConf();
        List<String> cmd = deviceLauncher != null ? List.of(deviceLauncher, "run", "--config", conf.toString())
                : List.of(java, "-Xmx256m", "-jar", jars.resolve("device-1.0.0-all.jar").toString(), "run", "--config", conf.toString());
        device = new Proc("device", cmd, work.resolve("device.log"),
                withProcessEnv(deviceLauncher != null ? Map.of("PIPELINE_ALLOW_ROOT", "1") : null)).start();
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
        if (externalMock()) {
            throw new IllegalStateException("scenario needs the stand-in's control port (" + cmd + "); not available with --mock-cmd");
        }
        try (Socket s = new Socket("127.0.0.1", mockCtl);
             PrintWriter out = new PrintWriter(s.getOutputStream(), true, StandardCharsets.UTF_8);
             BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8))) {
            out.println(cmd);
            String reply = in.readLine();
            event("mock_" + cmd.split(" ")[0], cmd + " -> " + reply);
            return reply;
        }
    }

    public void cutNetwork() throws Exception {
        if (netCmdTemplate != null) {
            event("network_cut", netCmd("cut"));
            return;
        }
        proxy.cut();
        event("network_cut", null);
    }

    public void restoreNetwork() throws Exception {
        if (netCmdTemplate != null) {
            event("network_restored", netCmd("restore"));
            return;
        }
        proxy.restore();
        event("network_restored", null);
    }

    String netCmd(String action) throws Exception {
        long t0 = tMs();
        Process p = new ProcessBuilder(shell(netCmdTemplate.replace("{action}", action))).redirectErrorStream(true).start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
        if (!p.waitFor(60, TimeUnit.SECONDS) || p.exitValue() != 0) {
            throw new IllegalStateException("network " + action + " command failed: " + out);
        }
        return "by command in " + (tMs() - t0) + " ms" + (out.isEmpty() ? "" : ": " + out);
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
            } else if (checkerCmdTemplate != null) {
                Map<String, Object> c = runChecker(dev, id);
                r.put("external_checker", c);
                pass &= Integer.valueOf(0).equals(c.get("exit_code"));
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

    Map<String, Object> runChecker(Path export, String session) throws Exception {
        String cmd = checkerCmdTemplate.replace("{export}", export.toString()).replace("{session}", session)
                .replace("{gt}", gtDir.resolve(session + ".json").toString());
        Path out = work.resolve("checker-" + session + ".txt");
        ProcessBuilder pb = new ProcessBuilder(shell(cmd)).redirectErrorStream(true).redirectOutput(out.toFile());
        Process p = pb.start();
        boolean done = p.waitFor(30, TimeUnit.MINUTES);
        String text = Files.readString(out);
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("command", cmd);
        m.put("exit_code", done ? p.exitValue() : -1);
        m.put("output_head", text.length() > 4000 ? text.substring(0, 4000) : text);
        return m;
    }

    public static final String LATENCY_CSV_HEADER = "session,window,window_end_ist,window_end_utc,latency_ms,device_batch_after_end_ms,cloud_received_after_end_ms,excluded_near_disruption";
    private static final DateTimeFormatter CSV_IST = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.of("Asia/Kolkata"));
    private static final DateTimeFormatter CSV_UTC = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.of("UTC"));

    public Map<String, Object> latency(String sessionId, List<long[]> disruptedMs, long windowsFromMs) throws Exception {
        return latency(sessionId, disruptedMs, windowsFromMs, null);
    }

    private static String afterEnd(JsonNode n, String field, long endWall) {
        return n.hasNonNull(field) ? Long.toString(Instant.parse(n.get(field).asText()).toEpochMilli() - endWall) : "";
    }

    public Map<String, Object> latency(String sessionId, List<long[]> disruptedMs, long windowsFromMs, List<String> csvRows) throws Exception {
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
                if (endT >= d[0] - 5000 && endT <= d[1] + 10_000) {
                    disturbed = true;
                }
            }
            long computed = Instant.parse(n.get("computed_at").asText()).toEpochMilli();
            long latency = computed - endWall;
            if (csvRows != null) {
                String batched = afterEnd(n, "device_captured_at", endWall);
                String received = afterEnd(n, "data_received_at", endWall);
                Instant end = Instant.ofEpochMilli(endWall);
                csvRows.add(sessionId + "," + w + "," + CSV_IST.format(end) + "," + CSV_UTC.format(end) + "," + latency + ","
                        + batched + "," + received + "," + (disturbed ? 1 : 0));
            }
            if (disturbed) {
                excluded++;
                continue;
            }
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
        m.put("definition", "computed_at (cloud wall clock) minus the wall time at which the window's last sample was generated by the mock; same machine clock. "
                + "Windows ending less than 5 s before a disruption, during it, or within 10 s after recovery are excluded (their 5 s deadline overlaps the outage) and counted separately.");
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
