package com.mayacardio.pipeline.tools.harness;

import com.fasterxml.jackson.databind.JsonNode;
import com.mayacardio.pipeline.core.Json;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class FailureTests {
    private static final long MAX_FILL_BYTES = 1L << 30;
    private final Map<String, String> opts;
    private final Path jars;
    private final Path workRoot;
    private final Path evidence;

    private FailureTests(Map<String, String> opts) {
        this.opts = opts;
        this.jars = Path.of(opts.getOrDefault("jars", "dist"));
        this.workRoot = Path.of(opts.getOrDefault("work", "evidence/raw"));
        this.evidence = Path.of(opts.getOrDefault("evidence", "evidence"));
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("usage: failtest <baseline|kill9|netcut|backlog|mockfaults|powercut|diskfull|clock|all> [--jars DIR] [--work DIR] [--evidence DIR] [options]");
            System.exit(2);
        }
        Map<String, String> opts = new LinkedHashMap<>();
        for (int i = 1; i < args.length; i++) {
            String k = args[i].substring(2);
            if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                opts.put(k, args[++i]);
            } else {
                opts.put(k, "true");
            }
        }
        FailureTests ft = new FailureTests(opts);
        List<String> names = args[0].equals("all")
                ? List.of("baseline", "kill9", "netcut", "mockfaults", "powercut", "backlog")
                : List.of(args[0].split(","));
        boolean all = true;
        for (String n : names) {
            all &= ft.runOne(n);
        }
        System.exit(all ? 0 : 1);
    }

    private int num(String k, int def) {
        return Integer.parseInt(opts.getOrDefault(k, Integer.toString(def)));
    }

    private boolean runOne(String name) throws Exception {
        String suffix = opts.getOrDefault("tag", "");
        String id = name + (suffix.isEmpty() ? "" : "-" + suffix);
        Path work = workRoot.resolve(id + "-" + Instant.now().toString().replace(':', '-').substring(0, 19));
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("scenario", id);
        result.put("started_at", Instant.now().toString());
        result.put("environment", environment());
        Rig rig = null;
        try {
            rig = new Rig(work, jars, Double.parseDouble(opts.getOrDefault("speed", "1")), opts.getOrDefault("mode", "signal"));
            rig.mockBufferSeconds = num("mock-buffer-seconds", 10);
            rig.externalCloudUrl = opts.get("cloud-url");
            rig.externalCloudToken = opts.get("cloud-token");
            if (rig.externalCloudUrl != null) {
                result.put("external_cloud", rig.externalCloudUrl);
                result.put("external_cloud_note", opts.getOrDefault("cloud-note", ""));
            }
            result.put("mock_buffer_seconds_of_data", rig.mockBufferSeconds);
            Map<String, Object> r = switch (name) {
                case "baseline" -> baseline(rig);
                case "kill9" -> kill9(rig);
                case "netcut" -> netcut(rig);
                case "backlog" -> backlog(rig);
                case "mockfaults" -> mockFaults(rig);
                case "powercut" -> powercut(rig);
                case "diskfull" -> diskFull(rig);
                case "clock" -> clock(rig);
                default -> throw new IllegalArgumentException("unknown scenario " + name);
            };
            result.putAll(r);
        } catch (Exception e) {
            result.put("pass", false);
            result.put("error", e.toString());
            e.printStackTrace();
        } finally {
            if (rig != null) {
                result.put("timeline", rig.timeline);
                rig.close();
            }
        }
        result.put("finished_at", Instant.now().toString());
        result.put("raw_dir", work.toString().replace('\\', '/'));
        Files.createDirectories(evidence);
        Path out = evidence.resolve(id + ".json");
        Files.writeString(out, Json.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(result) + "\n");
        System.out.println("== " + id + ": " + (Boolean.TRUE.equals(result.get("pass")) ? "PASS" : "FAIL") + " -> " + out);
        return Boolean.TRUE.equals(result.get("pass"));
    }

    static Map<String, Object> environment() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("os", System.getProperty("os.name") + " " + System.getProperty("os.version"));
        m.put("arch", System.getProperty("os.arch"));
        m.put("cpus_visible", Runtime.getRuntime().availableProcessors());
        m.put("java", System.getProperty("java.vendor") + " " + System.getProperty("java.version"));
        m.put("container_hint", Files.exists(Path.of("/.dockerenv")) ? "docker" : "none");
        m.put("mock", "mock-standin (NOT the official biosignal-mock)");
        m.put("cloud", "local cloud jar (SQLite) behind an in-process TCP proxy used to cut the network");
        return m;
    }

    private static long req(long v, String what) {
        if (v < 0) {
            throw new IllegalStateException("timed out waiting for " + what);
        }
        return v;
    }

    private void startAll(Rig rig) throws Exception {
        rig.startCloud();
        rig.startMock();
        rig.startDevice();
        long t = req(rig.waitMode("online", 30_000), "first online");
        rig.event("device_online", "after " + t + " ms");
    }

    private Map<String, Object> baseline(Rig rig) throws Exception {
        int seconds = num("seconds", 120);
        startAll(rig);
        long from = rig.tMs();
        rig.sleepUntil(from + seconds * 1000L);
        long synced = rig.waitSynced(60_000);
        rig.event("synced", synced + " ms");
        Map<String, Object> out = new LinkedHashMap<>();
        Map<String, Object> verify = rig.verifyAll(true);
        out.put("checks", verify);
        String sid = rig.status().get("session").asText();
        Map<String, Object> lat = rig.latency(sid, List.of(), from + 5000);
        out.put("online_latency", lat);
        out.put("pass", Boolean.TRUE.equals(verify.get("pass")) && ((Number) lat.get("windows_over_5s")).longValue() == 0);
        return out;
    }

    private Map<String, Object> kill9(Rig rig) throws Exception {
        int downMs = num("down-ms", 2000);
        startAll(rig);
        List<Map<String, Object>> kills = new ArrayList<>();
        long t0 = rig.tMs();
        for (int k = 0; k < 3; k++) {
            rig.sleepUntil(t0 + 15_000 + k * 25_000L);
            JsonNode before = rig.status();
            long next1 = before.path("current_session").path("next_index_1").asLong();
            rig.killDevice();
            long killedAt = rig.tMs();
            Thread.sleep(downMs);
            rig.startDevice();
            long resumed = rig.waitStatus(s -> s.path("daemon").asText().equals("running")
                    && s.path("pid").asLong() == rig.device.pid()
                    && s.path("current_session").path("next_index_1").asLong() > next1 + 400, 60_000);
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("killed_at_ms", killedAt);
            m.put("down_ms", downMs);
            m.put("next_index_1_before_kill", next1);
            m.put("ms_from_restart_to_collecting_past_kill_point", resumed);
            m.put("ms_from_restart_to_online", rig.waitMode("online", 30_000));
            kills.add(m);
        }
        rig.sleepUntil(rig.tMs() + 15_000);
        rig.event("synced", rig.waitSynced(60_000) + " ms");
        Map<String, Object> verify = rig.verifyAll(true);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("kills", kills);
        out.put("checks", verify);
        out.put("expectation", "restart within the mock's 10 s buffer: zero samples lost");
        out.put("pass", Boolean.TRUE.equals(verify.get("pass")) && noLosses(verify));
        return out;
    }

    @SuppressWarnings("unchecked")
    private static boolean noLosses(Map<String, Object> verify) {
        for (Map.Entry<String, Object> e : verify.entrySet()) {
            if (e.getValue() instanceof Map<?, ?> m && m.get("ground_truth_check") instanceof Map<?, ?> gt) {
                for (String s : new String[]{"sensor_1", "sensor_2"}) {
                    Map<String, Object> sm = (Map<String, Object>) gt.get(s);
                    if (((Number) sm.get("declared_lost_samples")).longValue() != 0) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    private Map<String, Object> netcut(Rig rig) throws Exception {
        int outage = num("outage", 60);
        startAll(rig);
        rig.sleepUntil(rig.tMs() + 20_000);
        long cutAt = rig.tMs();
        rig.cutNetwork();
        long toOffline = rig.waitMode("offline", 30_000);
        rig.event("device_offline", toOffline + " ms after cut");
        rig.sleepUntil(cutAt + outage * 1000L);
        JsonNode atRestore = rig.status();
        long restoreAt = rig.tMs();
        rig.restoreNetwork();
        long toOnline = rig.waitMode("online", 30_000);
        rig.event("device_online", toOnline + " ms after restore");
        long toSynced = rig.waitSynced(15 * 60_000);
        rig.event("cloud_copy_complete", toSynced + " ms after restore");
        long syncedAt = rig.tMs();
        rig.sleepUntil(rig.tMs() + 20_000);
        rig.waitSynced(60_000);
        Map<String, Object> verify = rig.verifyAll(true);
        String sid = rig.status().get("session").asText();
        Map<String, Object> lat = rig.latency(sid, List.of(new long[]{cutAt, syncedAt}), 5000);
        long offlineResults = Files.readAllLines(rig.work.resolve("results-" + sid + ".jsonl")).stream()
                .filter(l -> l.contains("\"mode\": \"offline\"")).count();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("outage_seconds", outage);
        out.put("ms_cut_to_offline", toOffline);
        out.put("ms_restore_to_online", toOnline);
        out.put("ms_restore_to_cloud_copy_complete", toSynced);
        out.put("backlog_at_restore", atRestore.path("backlog").asLong());
        out.put("offline_results_written", offlineResults);
        out.put("online_latency_outside_outage", lat);
        out.put("checks", verify);
        boolean pass = Boolean.TRUE.equals(verify.get("pass")) && toOffline >= 0 && toOffline <= 10_000
                && toOnline >= 0 && toOnline <= 10_000 && toSynced >= 0 && toSynced <= 900_000
                && ((Number) lat.get("windows_over_5s")).longValue() == 0 && noLosses(verify);
        out.put("pass", pass);
        return out;
    }

    private Map<String, Object> backlog(Rig rig) throws Exception {
        int dataMinutes = num("data-minutes", 120);
        double fast = Double.parseDouble(opts.getOrDefault("fast-speed", "20"));
        startAll(rig);
        rig.sleepUntil(rig.tMs() + 15_000);
        long cutAt = rig.tMs();
        rig.cutNetwork();
        req(rig.waitMode("offline", 30_000), "offline");
        long headAtCut = Json.read(rig.mockCmd("status")).get("generated").asLong();
        rig.mockCmd("speed " + fast);
        long target = headAtCut + dataMinutes * 60L * 400L;
        List<Map<String, Object>> progress = new ArrayList<>();
        while (true) {
            long head = Json.read(rig.mockCmd("status")).get("generated").asLong();
            JsonNode st = rig.status();
            Map<String, Object> p = new LinkedHashMap<>();
            p.put("t_ms", rig.tMs());
            p.put("mock_generated", head);
            p.put("device_next_index_1", st.path("current_session").path("next_index_1").asLong());
            p.put("pending_in_memory", st.path("pending_in_memory").asLong());
            progress.add(p);
            if (head >= target) {
                break;
            }
            Thread.sleep(15_000);
        }
        rig.mockCmd("speed 1");
        rig.waitStatus(s -> s.path("current_session").path("next_index_1").asLong() >= target - 4000, 120_000);
        JsonNode atRestore = rig.status();
        long bytesBefore = atRestore.path("http_bytes_sent").asLong();
        long restoreAt = rig.tMs();
        rig.restoreNetwork();
        long toOnline = rig.waitMode("online", 30_000);
        List<Map<String, Object>> drain = new ArrayList<>();
        long toSynced = -1;
        while (rig.tMs() - restoreAt < 20 * 60_000L) {
            JsonNode st = rig.status();
            Map<String, Object> p = new LinkedHashMap<>();
            p.put("t_ms_after_restore", rig.tMs() - restoreAt);
            p.put("backlog", st.path("backlog").asLong());
            p.put("catchup_backlog_samples", st.path("catchup_backlog_samples").asLong());
            drain.add(p);
            if (st.path("backlog").asLong() <= rig.inFlightTolerance() && st.path("catchup_backlog_samples").asLong() == 0 && "online".equals(st.path("mode").asText())) {
                toSynced = rig.tMs() - restoreAt;
                break;
            }
            Thread.sleep(2000);
        }
        rig.event("cloud_copy_complete", toSynced + " ms after restore");
        JsonNode after = rig.status();
        rig.sleepUntil(rig.tMs() + 10_000);
        Map<String, Object> verify = rig.verifyAll(true);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("simulated_outage_data_minutes", dataMinutes);
        out.put("generation_speed_while_offline", fast);
        out.put("samples_backlog_at_restore", atRestore.path("backlog").asLong());
        out.put("ms_restore_to_online", toOnline);
        out.put("ms_restore_to_cloud_copy_complete", toSynced);
        out.put("http_bytes_sent_during_catchup", after.path("http_bytes_sent").asLong() - bytesBefore);
        out.put("generation_progress", progress);
        out.put("drain_progress", drain);
        out.put("note", "cloud runs on the same machine; network bandwidth is not limited. Bytes sent are reported so time at a given bandwidth can be estimated.");
        out.put("checks", verify);
        out.put("pass", Boolean.TRUE.equals(verify.get("pass")) && toSynced >= 0 && toSynced <= 900_000 && toOnline >= 0 && toOnline <= 10_000);
        return out;
    }

    private Map<String, Object> mockFaults(Rig rig) throws Exception {
        startAll(rig);
        long t0 = rig.tMs();
        Object[][] plan = {
                {10, "corrupt 10"}, {16, "duplicate 10"}, {22, "stall 3"}, {32, "outage 2 8"}, {48, "outage 1 5"},
                {60, "stall 15"}, {90, "restart"}};
        for (Object[] step : plan) {
            rig.sleepUntil(t0 + ((Integer) step[0]) * 1000L);
            rig.mockCmd((String) step[1]);
        }
        rig.sleepUntil(t0 + 110_000);
        rig.event("synced", rig.waitSynced(120_000) + " ms");
        Map<String, Object> verify = rig.verifyAll(true);
        JsonNode st = rig.status();
        Map<String, Object> counters = new LinkedHashMap<>();
        for (String k : new String[]{"corrupt_replies", "timeouts", "reconnects", "stale_replies", "replies_ok"}) {
            counters.put(k, st.path(k).asLong());
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("injected", "corrupt x10, duplicate x10, stall 3 s, sensor 2 outage 8 s, sensor 1 outage 5 s, stall 15 s (> 10 s buffer), session restart");
        out.put("device_counters", counters);
        out.put("checks", verify);
        out.put("expectation", "losses only where the mock could not deliver (outages, the >10 s stall, unread tail at restart), each declared with a reason; no silent loss, no duplicates");
        out.put("pass", Boolean.TRUE.equals(verify.get("pass")));
        return out;
    }

    private Map<String, Object> powercut(Rig rig) throws Exception {
        startAll(rig);
        rig.sleepUntil(rig.tMs() + 30_000);
        String oldSession = rig.status().get("session").asText();
        JsonNode gtBefore = Json.read(Files.readString(rig.gtDir.resolve(oldSession + ".json")));
        long started = gtBefore.get("started_at_ms").asLong();
        double sp = gtBefore.get("speed").asDouble();
        rig.killDevice();
        long cutWall = System.currentTimeMillis();
        rig.killMock();
        long generatedAtCut = (long) Math.floor((cutWall - started) / 1000.0 * 400 * sp);
        Thread.sleep(3000);
        rig.startMock();
        rig.startDevice();
        req(rig.waitMode("online", 60_000), "online after power return");
        rig.sleepUntil(rig.tMs() + 20_000);
        rig.event("synced", rig.waitSynced(60_000) + " ms");
        Map<String, Object> verify = rig.verifyAll(true);
        Map<String, Object> out = new LinkedHashMap<>();
        double worst = 0;
        for (JsonNode s : rig.sessions()) {
            if (s.get("session").asText().equals(oldSession)) {
                for (int sensor = 1; sensor <= 2; sensor++) {
                    long next = s.get("next_index_" + sensor).asLong();
                    double lostSec = (generatedAtCut - next) / 400.0 / sp;
                    out.put("sensor_" + sensor + "_samples_generated_but_not_saved", generatedAtCut - next);
                    out.put("sensor_" + sensor + "_seconds_lost", lostSec);
                    worst = Math.max(worst, lostSec);
                }
            }
        }
        out.put("simulation", "SIGKILL of the device daemon and the mock at the same instant, restart 3 s later. "
                + "This does NOT drop the OS page cache, so it is weaker than a real power pull; it does exercise the restart path and the tail-loss accounting.");
        out.put("worst_seconds_lost", worst);
        out.put("stated_bound_seconds", 1.0);
        out.put("checks", verify);
        out.put("pass", Boolean.TRUE.equals(verify.get("pass")) && worst <= 1.0);
        return out;
    }

    private Map<String, Object> diskFull(Rig rig) throws Exception {
        String small = opts.get("small-fs");
        if (small == null) {
            return Map.of("pass", false, "skipped", "needs --small-fs DIR on a small dedicated filesystem (e.g. a 64 MB tmpfs)");
        }
        int hold = num("hold", 20);
        Path fs = Path.of(small);
        long total = Files.getFileStore(fs).getTotalSpace();
        if (total > MAX_FILL_BYTES) {
            return Map.of("pass", false, "skipped", "--small-fs " + small + " is " + (total >> 20)
                    + " MB; refusing to fill a filesystem larger than " + (MAX_FILL_BYTES >> 20) + " MB");
        }
        rig.dataDir = fs.resolve("device-data-" + System.nanoTime());
        if (opts.containsKey("memory-samples")) {
            rig.deviceExtra.put("disk.full.memory.samples", opts.get("memory-samples"));
        }
        startAll(rig);
        rig.sleepUntil(rig.tMs() + 15_000);
        Path filler = fs.resolve("filler.bin");
        long written = 0;
        byte[] mb = new byte[1 << 20];
        try (OutputStream out = Files.newOutputStream(filler)) {
            while (written < MAX_FILL_BYTES) {
                out.write(mb);
                written += mb.length;
            }
        } catch (IOException e) {
            rig.event("disk_filled", "filler " + (written >> 20) + " MB: " + e.getMessage());
        }
        long toDiskFull = rig.waitStatus(s -> s.path("disk_full").asBoolean(), 30_000);
        rig.event("device_reports_disk_full", toDiskFull + " ms");
        Thread.sleep(hold * 1000L);
        JsonNode during = rig.status();
        Files.delete(filler);
        rig.event("disk_space_freed", null);
        long toRecover = rig.waitStatus(s -> !s.path("disk_full").asBoolean() && s.path("pending_in_memory").asLong() == 0, 60_000);
        rig.event("device_recovered", toRecover + " ms");
        rig.sleepUntil(rig.tMs() + 10_000);
        rig.waitSynced(60_000);
        boolean alive = rig.device.alive();
        Map<String, Object> verify = rig.verifyAll(true);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("hold_seconds", hold);
        out.put("memory_buffer_samples_per_sensor", opts.getOrDefault("memory-samples", "48000 (default)"));
        out.put("ms_to_detect_disk_full", toDiskFull);
        out.put("pending_in_memory_while_full", during.path("pending_in_memory").asLong());
        out.put("ms_to_recover_after_space_freed", toRecover);
        out.put("daemon_survived", alive);
        out.put("checks", verify);
        out.put("pass", alive && toDiskFull >= 0 && toRecover >= 0 && Boolean.TRUE.equals(verify.get("pass")));
        return out;
    }

    private Map<String, Object> clock(Rig rig) throws Exception {
        if (!opts.containsKey("allow-clock-change")) {
            return Map.of("pass", false, "skipped", "needs --allow-clock-change on Linux with CAP_SYS_TIME (changes the system clock)");
        }
        startAll(rig);
        rig.sleepUntil(rig.tMs() + 20_000);
        List<Map<String, Object>> jumps = new ArrayList<>();
        long[] deltas = {3600, -7200, 3600};
        for (long d : deltas) {
            long before = System.currentTimeMillis();
            Process p = new ProcessBuilder("date", "-s", "@" + (before / 1000 + d)).redirectErrorStream(true).start();
            String outp = new String(p.getInputStream().readAllBytes()).trim();
            int code = p.waitFor();
            rig.event("clock_jump", d + " s -> exit " + code + " " + outp);
            Map<String, Object> j = new LinkedHashMap<>();
            j.put("delta_seconds", d);
            j.put("exit", code);
            jumps.add(j);
            long switched = rig.waitMode("offline", 20_000);
            j.put("spurious_offline_switch_ms", switched);
        }
        rig.sleepUntil(rig.tMs() + 10_000);
        rig.event("synced", rig.waitSynced(60_000) + " ms");
        Map<String, Object> verify = rig.verifyAll(true);
        boolean noSwitch = jumps.stream().allMatch(j -> ((Number) j.get("spurious_offline_switch_ms")).longValue() < 0);
        boolean jumped = jumps.stream().allMatch(j -> ((Number) j.get("exit")).intValue() == 0);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("jumps", jumps);
        out.put("no_spurious_mode_switch", noSwitch);
        out.put("checks", verify);
        out.put("pass", jumped && noSwitch && Boolean.TRUE.equals(verify.get("pass")) && noLosses(verify));
        return out;
    }
}
