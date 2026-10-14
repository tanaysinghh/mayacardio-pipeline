package com.mayacardio.pipeline.cloud;

import com.mayacardio.pipeline.cloud.service.IngestService;
import com.mayacardio.pipeline.cloud.service.ProcessingService;
import com.mayacardio.pipeline.cloud.service.SessionRegistry;
import com.mayacardio.pipeline.cloud.store.CloudStore;
import com.mayacardio.pipeline.cloud.store.JdbcCloudStore;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.sqlite.SQLiteConfig;

import javax.sql.DataSource;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

@Configuration
public class CloudConfig {

    @Bean(destroyMethod = "close")
    public HikariDataSource cloudDataSource(@Value("${cloud.storage:sqlite}") String storage,
                                            @Value("${cloud.sqlite.path:./cloud-data/cloud.db}") String sqlitePath,
                                            @Value("${cloud.jdbc.url:}") String jdbcUrl,
                                            @Value("${cloud.jdbc.user:}") String user,
                                            @Value("${cloud.jdbc.password:}") String password,
                                            @Value("${cloud.jdbc.pool:4}") int pool) throws Exception {
        HikariConfig hc = new HikariConfig();
        hc.setPoolName("cloud-store");
        if ("postgres".equalsIgnoreCase(storage)) {
            String[] parsed = toJdbc(jdbcUrl, user, password);
            hc.setJdbcUrl(parsed[0]);
            if (!parsed[1].isEmpty()) {
                hc.setUsername(parsed[1]);
            }
            if (!parsed[2].isEmpty()) {
                hc.setPassword(parsed[2]);
            }
            hc.setMaximumPoolSize(pool);
            hc.setMinimumIdle(1);
            hc.setKeepaliveTime(60_000);
            hc.setMaxLifetime(300_000);
            hc.setConnectionTimeout(15_000);
        } else {
            Path p = Path.of(sqlitePath).toAbsolutePath();
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
            throw new IllegalStateException("cloud.storage=postgres requires CLOUD_JDBC_URL");
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

    @Bean
    public CloudStore cloudStore(DataSource ds, @Value("${cloud.storage:sqlite}") String storage) {
        JdbcCloudStore store = new JdbcCloudStore(ds,
                "postgres".equalsIgnoreCase(storage) ? JdbcCloudStore.Dialect.POSTGRES : JdbcCloudStore.Dialect.SQLITE);
        store.init();
        return store;
    }

    @Bean
    public SessionRegistry sessionRegistry(CloudStore store) {
        return new SessionRegistry(store);
    }

    @Bean(destroyMethod = "close")
    public ProcessingService processingService(SessionRegistry registry) {
        return new ProcessingService(registry);
    }

    @Bean
    public IngestService ingestService(SessionRegistry registry, ProcessingService processing) {
        return new IngestService(registry, processing);
    }

    @Bean
    public TokenFilter tokenFilter(@Value("${cloud.api.token:}") String token) {
        return new TokenFilter(token);
    }
}
