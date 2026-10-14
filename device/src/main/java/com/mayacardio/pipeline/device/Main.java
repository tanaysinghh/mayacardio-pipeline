package com.mayacardio.pipeline.device;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mayacardio.pipeline.core.ExportFormat;
import com.mayacardio.pipeline.core.Json;
import com.mayacardio.pipeline.core.LossRange;
import com.mayacardio.pipeline.core.ResultJson;
import com.mayacardio.pipeline.device.net.CloudClient;
import com.mayacardio.pipeline.device.store.DeviceStore;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class Main {

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            usage();
            System.exit(2);
        }
        Map<String, String> opts = new HashMap<>();
        for (int i = 1; i < args.length; i++) {
            if (args[i].startsWith("--") && i + 1 < args.length) {
                opts.put(args[i].substring(2), args[++i]);
            } else {
                System.err.println("unexpected argument: " + args[i]);
                usage();
                System.exit(2);
            }
        }
        DeviceConfig cfg = DeviceConfig.load(opts.containsKey("config") ? Path.of(opts.get("config")) : null);
        int code;
        switch (args[0]) {
            case "run" -> {
                new Daemon(cfg).run();
                code = 0;
            }
            case "status" -> code = status(cfg);
            case "export" -> code = export(cfg, opts);
            case "results" -> code = results(cfg, opts);
            case "sessions" -> code = sessions(cfg);
            case "version" -> {
                System.out.println("{\"pipeline\": \"1.0.0\"}");
                code = 0;
            }
            default -> {
                usage();
                code = 2;
            }
        }
        System.exit(code);
    }

    private static void usage() {
        System.err.println("""
                usage: pipeline <command> [--config FILE]
                  run                                      start the collector/uploader daemon
                  status                                   one JSON line: mode, samples saved, samples in cloud, backlog
                  export --session ID --from device|cloud  JSON lines: samples then losses
                  results --session ID [--from both|device|cloud] [--mode online|offline]
                  sessions                                 list sessions on the device
                  version""");
    }

    private static void fail(String msg) {
        System.err.println("{\"error\": " + Json.quote(msg) + "}");
    }

    private static boolean daemonRunning(DeviceConfig cfg) {
        try {
            FileLock lock = Daemon.lock(cfg.dataDir());
            if (lock == null) {
                return true;
            }
            lock.release();
            lock.channel().close();
            return false;
        } catch (IOException e) {
            return false;
        }
    }

    static int status(DeviceConfig cfg) throws Exception {
        boolean running = daemonRunning(cfg);
        Path file = cfg.runDir().resolve("status.json");
        if (running && Files.isRegularFile(file)) {
            for (int attempt = 0; attempt < 5; attempt++) {
                try {
                    JsonNode n = Json.read(Files.readString(file));
                    System.out.println(Json.write(n));
                    return 0;
                } catch (IOException | IllegalArgumentException e) {
                    Thread.sleep(50);
                }
            }
        }
        if (!Files.exists(cfg.dbPath())) {
            System.out.println("{\"mode\": \"stopped\", \"daemon\": \"" + (running ? "starting" : "stopped")
                    + "\", \"session\": null, \"samples_saved\": 0, \"samples_in_cloud\": 0, \"backlog\": 0}");
            return 0;
        }
        try (DeviceStore store = DeviceStore.open(cfg.dbPath())) {
            long saved = 0;
            long cloud = 0;
            long lossRanges = 0;
            String latest = null;
            for (DeviceStore.SessionInfo s : store.sessions()) {
                long s1 = store.stream(s.sid(), 1).storedCount();
                long s2 = store.stream(s.sid(), 2).storedCount();
                long[] sync = store.syncState(s.sid());
                saved += s1 + s2;
                cloud += Math.min(sync[0], s1) + Math.min(sync[1], s2);
                lossRanges += store.lossCount(s.sid());
                latest = s.sessionId();
            }
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("mode", "stopped");
            m.put("daemon", running ? "starting" : "stopped");
            m.put("session", latest);
            m.put("samples_saved", saved);
            m.put("samples_in_cloud", cloud);
            m.put("backlog", Math.max(0, saved - cloud));
            m.put("loss_ranges", lossRanges);
            m.put("note", "daemon not running; cloud count is the last value persisted by the daemon");
            System.out.println(Json.write(m));
        }
        return 0;
    }

    private static DeviceStore.SessionInfo resolveSession(DeviceStore store, String id) throws Exception {
        if (id == null) {
            return null;
        }
        if (id.equals("current") || id.equals("latest")) {
            return store.latestSession();
        }
        return store.session(id);
    }

    private static BufferedWriter stdout() {
        return new BufferedWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8), 1 << 16);
    }

    private static void line(BufferedWriter w, String s) {
        try {
            w.write(s);
            w.write('\n');
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static int export(DeviceConfig cfg, Map<String, String> opts) throws Exception {
        String id = opts.get("session");
        String from = opts.getOrDefault("from", "device");
        if (id == null) {
            fail("--session is required");
            return 2;
        }
        if (from.equals("cloud")) {
            if (cfg.cloudUrl().isEmpty()) {
                fail("no cloud.url configured");
                return 1;
            }
            String sessionId = id;
            if (id.equals("current") || id.equals("latest")) {
                try (DeviceStore store = DeviceStore.open(cfg.dbPath())) {
                    DeviceStore.SessionInfo s = store.latestSession();
                    if (s == null) {
                        fail("no sessions on device");
                        return 1;
                    }
                    sessionId = s.sessionId();
                }
            }
            CloudClient cloud = new CloudClient(cfg.cloudUrl(), cfg.cloudToken(), null);
            BufferedWriter w = stdout();
            try {
                cloud.streamLines("/api/v1/sessions/" + sessionId + "/export", 600_000, l -> line(w, l));
            } catch (IOException e) {
                w.flush();
                fail("cloud export failed: " + e.getMessage());
                return 1;
            }
            w.flush();
            return 0;
        }
        if (!from.equals("device")) {
            fail("--from must be device or cloud");
            return 2;
        }
        try (DeviceStore store = DeviceStore.open(cfg.dbPath())) {
            DeviceStore.SessionInfo s = resolveSession(store, id);
            if (s == null) {
                fail("unknown session " + id);
                return 1;
            }
            BufferedWriter w = stdout();
            for (int sensor = 1; sensor <= 2; sensor++) {
                store.forEachSample(s.sid(), sensor, (sen, idx, v) -> {
                    w.write(ExportFormat.sampleLine(sen, idx, v));
                    w.write('\n');
                });
            }
            for (LossRange l : store.losses(s.sid())) {
                line(w, ExportFormat.lossLine(l));
            }
            w.flush();
        }
        return 0;
    }

    static int results(DeviceConfig cfg, Map<String, String> opts) throws Exception {
        String id = opts.get("session");
        String from = opts.getOrDefault("from", "both");
        String mode = opts.get("mode");
        if (id == null) {
            fail("--session is required");
            return 2;
        }
        List<String> deviceLines = new ArrayList<>();
        String sessionId = id;
        try (DeviceStore store = DeviceStore.open(cfg.dbPath())) {
            DeviceStore.SessionInfo s = resolveSession(store, id);
            if (s != null) {
                sessionId = s.sessionId();
                if (!from.equals("cloud")) {
                    for (DeviceStore.StoredResult r : store.results(s.sid(), false, Integer.MAX_VALUE)) {
                        deviceLines.add(r.body());
                    }
                }
            } else if (from.equals("device")) {
                fail("unknown session " + id);
                return 1;
            }
        }
        Map<String, String> byKey = new java.util.TreeMap<>();
        for (String l : deviceLines) {
            JsonNode n = Json.read(l);
            byKey.put(key(n), l);
        }
        int code = 0;
        if (!from.equals("device") && !cfg.cloudUrl().isEmpty()) {
            CloudClient cloud = new CloudClient(cfg.cloudUrl(), cfg.cloudToken(), null);
            String path = "/api/v1/sessions/" + sessionId + "/results" + (from.equals("cloud") && mode != null ? "?mode=" + mode : "");
            try {
                cloud.streamLines(path, 600_000, l -> {
                    JsonNode n = Json.read(l);
                    String k = key(n);
                    if (from.equals("cloud") || ResultJson.ONLINE.equals(n.path("mode").asText()) || !byKey.containsKey(k)) {
                        byKey.put(k, l);
                    }
                });
            } catch (IOException e) {
                fail("cloud results unavailable (" + e.getMessage() + "); printing device results only");
                code = from.equals("cloud") ? 1 : 0;
            }
        } else if (from.equals("cloud")) {
            fail("no cloud.url configured");
            return 1;
        }
        BufferedWriter w = stdout();
        for (String l : byKey.values()) {
            if (mode != null) {
                JsonNode n = Json.read(l);
                if (!mode.equals(n.path("mode").asText())) {
                    continue;
                }
            }
            line(w, l);
        }
        w.flush();
        return code;
    }

    private static String key(JsonNode n) {
        return String.format("%012d|%s", n.path("window").asLong(), n.path("mode").asText());
    }

    static int sessions(DeviceConfig cfg) throws Exception {
        try (DeviceStore store = DeviceStore.open(cfg.dbPath())) {
            BufferedWriter w = stdout();
            for (DeviceStore.SessionInfo s : store.sessions()) {
                ObjectNode n = Json.MAPPER.createObjectNode();
                n.put("session", s.sessionId());
                n.put("first_seen", s.firstSeen());
                n.put("closed", s.closed());
                n.put("close_reason", s.closeReason());
                for (int sensor = 1; sensor <= 2; sensor++) {
                    DeviceStore.StreamState st = store.stream(s.sid(), sensor);
                    n.put("saved_" + sensor, st.storedCount());
                    n.put("next_index_" + sensor, st.nextIndex());
                }
                n.put("loss_ranges", store.lossCount(s.sid()));
                long[] sync = store.syncState(s.sid());
                n.put("cloud_complete", sync[3] != 0);
                line(w, Json.write(n));
            }
            w.flush();
        }
        return 0;
    }
}
