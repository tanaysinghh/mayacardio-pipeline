package com.mayacardio.pipeline.device;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

public final class DeviceConfig {
    public static final Path DEFAULT_CONFIG = Path.of("/etc/pipeline/pipeline.conf");

    private final Properties props;
    private final Map<String, String> env;

    private DeviceConfig(Properties props, Map<String, String> env) {
        this.props = props;
        this.env = env;
    }

    public static DeviceConfig load(Path explicit) throws IOException {
        Properties p = new Properties();
        Path file = explicit;
        if (file != null && !Files.isRegularFile(file)) {
            throw new IOException("config file not found: " + file);
        }
        if (file == null && System.getenv("PIPELINE_CONFIG") != null && Files.isRegularFile(Path.of(System.getenv("PIPELINE_CONFIG")))) {
            file = Path.of(System.getenv("PIPELINE_CONFIG"));
        }
        if (file == null && Files.isRegularFile(DEFAULT_CONFIG)) {
            file = DEFAULT_CONFIG;
        }
        if (file != null) {
            try (Reader r = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
                p.load(r);
            }
        }
        return new DeviceConfig(p, System.getenv());
    }

    public static DeviceConfig of(Properties p) {
        return new DeviceConfig(p, Map.of());
    }

    public String get(String key, String def) {
        String sys = System.getProperty("pipeline." + key);
        if (sys != null) {
            return sys.trim();
        }
        String envKey = "PIPELINE_" + key.toUpperCase(Locale.ROOT).replace('.', '_');
        String e = env.get(envKey);
        if (e != null) {
            return e.trim();
        }
        String v = props.getProperty(key);
        return v == null ? def : v.trim();
    }

    public long getLong(String key, long def) {
        return Long.parseLong(get(key, Long.toString(def)));
    }

    public int getInt(String key, int def) {
        return Integer.parseInt(get(key, Integer.toString(def)));
    }

    public String mockHost() {
        return get("mock.host", "127.0.0.1");
    }

    public int mockPort() {
        return getInt("mock.port", 7100);
    }

    public Path dataDir() {
        return Path.of(get("data.dir", "/var/lib/pipeline"));
    }

    public Path dbPath() {
        return dataDir().resolve("pipeline.db");
    }

    public Path runDir() {
        return Path.of(get("run.dir", dataDir().resolve("run").toString()));
    }

    public String cloudUrl() {
        String u = get("cloud.url", "");
        return u.endsWith("/") ? u.substring(0, u.length() - 1) : u;
    }

    public String cloudToken() {
        return get("cloud.token", "");
    }

    public long pollIntervalMs() {
        return getLong("poll.interval.ms", 500);
    }

    public long replyTimeoutMs() {
        return getLong("mock.reply.timeout.ms", 2000);
    }

    public long reserveBytes() {
        return getLong("disk.reserve.bytes", 8L * 1024 * 1024);
    }

    public long minFreeBytes() {
        return getLong("disk.min.free.bytes", 16L * 1024 * 1024);
    }

    public long resumeFreeBytes() {
        return getLong("disk.resume.free.bytes", 48L * 1024 * 1024);
    }

    public int memoryBufferSamples() {
        return getInt("disk.full.memory.samples", 48_000);
    }

    public long heartbeatIntervalMs() {
        return getLong("heartbeat.interval.ms", 2000);
    }

    public long heartbeatTimeoutMs() {
        return getLong("heartbeat.timeout.ms", 1500);
    }

    public int heartbeatFailThreshold() {
        return getInt("heartbeat.fail.threshold", 2);
    }

    public int heartbeatOkThreshold() {
        return getInt("heartbeat.ok.threshold", 2);
    }

    public long liveIntervalMs() {
        return getLong("live.interval.ms", 1000);
    }

    public long liveSpanSamples() {
        return getLong("live.span.samples", 4000);
    }

    public int catchupChunkSamples() {
        return getInt("catchup.chunk.samples", 40_000);
    }

    public int catchupParallel() {
        return getInt("catchup.parallel", 2);
    }

    public long uploadTimeoutMs() {
        return getLong("upload.timeout.ms", 20_000);
    }

    public long statusIntervalMs() {
        return getLong("status.interval.ms", 1000);
    }
}
