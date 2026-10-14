package com.mayacardio.pipeline.cloud;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.sqlite.SQLiteConfig;

import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public final class CloudConfig {
    private final Map<String, String> env;

    public CloudConfig(Map<String, String> env) {
        this.env = env;
    }

    public String get(String key, String def) {
        String v = env.get(key);
        return v == null || v.isBlank() ? def : v.trim();
    }

    public int port() {
        return Integer.parseInt(get("PORT", "8080"));
    }

    public boolean postgres() {
        return "postgres".equalsIgnoreCase(get("CLOUD_STORAGE", "sqlite"));
    }

    public String token() {
        return get("CLOUD_API_TOKEN", "");
    }

    public int httpThreads() {
        return Integer.parseInt(get("CLOUD_HTTP_THREADS", "8"));
    }

    public HikariDataSource dataSource() throws IOException {
        HikariConfig hc = new HikariConfig();
        hc.setPoolName("cloud-store");
        int pool = Integer.parseInt(get("CLOUD_JDBC_POOL", "4"));
        if (postgres()) {
            String[] parsed = toJdbc(get("CLOUD_JDBC_URL", ""), get("CLOUD_JDBC_USER", ""), get("CLOUD_JDBC_PASSWORD", ""));
            hc.setJdbcUrl(parsed[0]);
            if (!parsed[1].isEmpty()) {
                hc.setUsername(parsed[1]);
            }
            if (!parsed[2].isEmpty()) {
                hc.setPassword(parsed[2]);
            }
            hc.setMaximumPoolSize(pool);
            hc.setMinimumIdle(0);
            hc.setIdleTimeout(120_000);
            hc.setMaxLifetime(240_000);
            hc.setConnectionTimeout(15_000);
            hc.setInitializationFailTimeout(-1);
        } else {
            Path p = Path.of(get("CLOUD_SQLITE_PATH", "./cloud-data/cloud.db")).toAbsolutePath();
            Files.createDirectories(p.getParent());
            SQLiteConfig sc = new SQLiteConfig();
            sc.setJournalMode(SQLiteConfig.JournalMode.WAL);
            sc.setSynchronous(SQLiteConfig.SynchronousMode.FULL);
            sc.setBusyTimeout(15_000);
            hc.setJdbcUrl("jdbc:sqlite:" + p);
            hc.setDataSourceProperties(sc.toProperties());
            hc.setMaximumPoolSize(Math.max(2, pool));
        }
        return new HikariDataSource(hc);
    }

    static String[] toJdbc(String url, String user, String password) {
        if (url == null || url.isBlank()) {
            throw new IllegalStateException("CLOUD_STORAGE=postgres requires CLOUD_JDBC_URL");
        }
        if (url.startsWith("jdbc:")) {
            return new String[]{url, user, password};
        }
        URI u = URI.create(url.replaceFirst("^postgres(ql)?://", "http://"));
        String u2 = user;
        String p2 = password;
        if (u.getRawUserInfo() != null) {
            String[] ui = u.getRawUserInfo().split(":", 2);
            if (u2.isEmpty()) {
                u2 = URLDecoder.decode(ui[0], StandardCharsets.UTF_8);
            }
            if (p2.isEmpty() && ui.length > 1) {
                p2 = URLDecoder.decode(ui[1], StandardCharsets.UTF_8);
            }
        }
        String query = u.getRawQuery() == null ? "sslmode=require" : u.getRawQuery();
        String jdbc = "jdbc:postgresql://" + u.getHost() + (u.getPort() > 0 ? ":" + u.getPort() : "") + u.getRawPath() + "?" + query;
        return new String[]{jdbc, u2, p2};
    }
}
