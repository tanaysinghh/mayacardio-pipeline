package com.mayacardio.pipeline.cloud;

import com.mayacardio.pipeline.cloud.service.IngestService;
import com.mayacardio.pipeline.cloud.service.ProcessingService;
import com.mayacardio.pipeline.cloud.service.SessionRegistry;
import com.mayacardio.pipeline.cloud.store.JdbcCloudStore;
import com.sun.net.httpserver.HttpServer;
import com.zaxxer.hikari.HikariDataSource;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class CloudMain implements AutoCloseable {
    private final HttpServer server;
    private final HikariDataSource dataSource;
    private final ProcessingService processing;
    private final ExecutorService executor;

    private CloudMain(HttpServer server, HikariDataSource dataSource, ProcessingService processing, ExecutorService executor) {
        this.server = server;
        this.dataSource = dataSource;
        this.processing = processing;
        this.executor = executor;
    }

    public static CloudMain start(Map<String, String> env) throws Exception {
        CloudConfig cfg = new CloudConfig(env);
        HikariDataSource ds = cfg.dataSource();
        JdbcCloudStore store = new JdbcCloudStore(ds, cfg.postgres() ? JdbcCloudStore.Dialect.POSTGRES : JdbcCloudStore.Dialect.SQLITE);
        store.init();
        SessionRegistry registry = new SessionRegistry(store);
        ProcessingService processing = new ProcessingService(registry);
        IngestService ingest = new IngestService(registry, processing);
        HttpServer server = HttpServer.create(new InetSocketAddress(cfg.port()), 64);
        ExecutorService executor = Executors.newFixedThreadPool(cfg.httpThreads(), r -> {
            Thread t = new Thread(r, "http");
            t.setDaemon(true);
            return t;
        });
        server.setExecutor(executor);
        server.createContext("/", new CloudHttp(registry, ingest, processing, store, cfg.token()));
        server.start();
        return new CloudMain(server, ds, processing, executor);
    }

    public int port() {
        return server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(1);
        executor.shutdownNow();
        processing.close();
        dataSource.close();
    }

    public static void main(String[] args) throws Exception {
        CloudMain app = start(System.getenv());
        System.out.println(Instant.now() + " cloud listening on port " + app.port() + " (storage "
                + (new CloudConfig(System.getenv()).postgres() ? "postgres" : "sqlite") + ")");
        Runtime.getRuntime().addShutdownHook(new Thread(app::close, "shutdown"));
        Thread.currentThread().join();
    }
}
