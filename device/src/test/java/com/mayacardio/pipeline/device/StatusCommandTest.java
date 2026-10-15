package com.mayacardio.pipeline.device;

import com.fasterxml.jackson.databind.JsonNode;
import com.mayacardio.pipeline.core.Json;
import com.mayacardio.pipeline.mock.Generator;
import com.mayacardio.pipeline.mock.MockServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StatusCommandTest {
    @TempDir
    Path dir;
    private MockServer mock;
    private Daemon daemon;
    private Thread daemonThread;

    @AfterEach
    void stop() throws Exception {
        if (daemon != null) {
            daemon.stop();
            daemonThread.join(20_000);
        }
        if (mock != null) {
            mock.close();
        }
    }

    private List<String> runStatus(Path config) throws Exception {
        List<String> cmd = List.of(ProcessHandle.current().info().command().orElse("java"), "-cp", System.getProperty("java.class.path"),
                Main.class.getName(), "status", "--config", config.toString());
        Process p = new ProcessBuilder(cmd).redirectError(ProcessBuilder.Redirect.DISCARD).start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue(p.waitFor(60, TimeUnit.SECONDS), "pipeline status did not exit");
        assertEquals(0, p.exitValue());
        List<String> lines = new ArrayList<>();
        for (String l : out.split("\\R")) {
            if (!l.isBlank()) {
                lines.add(l);
            }
        }
        assertEquals(1, lines.size(), "pipeline status must print exactly one line: " + lines);
        return lines;
    }

    private JsonNode status(Path config) throws Exception {
        return Json.read(runStatus(config).get(0));
    }

    private JsonNode statusWhenSaved(Path config, long above, long deadlineMs) throws Exception {
        long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(deadlineMs);
        JsonNode last = null;
        while (System.nanoTime() < end) {
            last = status(config);
            if ("running".equals(last.path("daemon").asText()) && last.path("samples_saved").asLong() > above) {
                return last;
            }
            Thread.sleep(300);
        }
        throw new AssertionError("pipeline status never reported more than " + above + " saved samples; last: " + last);
    }

    @Test
    void statusCommandReportsTheRunningDaemon() throws Exception {
        mock = new MockServer(new Generator("signal", 7), 1.0, 0, 10, 0, -1, null);
        mock.start();
        Properties p = new Properties();
        p.setProperty("mock.host", "127.0.0.1");
        p.setProperty("mock.port", Integer.toString(mock.port()));
        p.setProperty("data.dir", dir.resolve("data").toString());
        p.setProperty("run.dir", dir.resolve("run").toString());
        p.setProperty("cloud.url", "");
        Path config = dir.resolve("pipeline.conf");
        try (Writer w = Files.newBufferedWriter(config, StandardCharsets.UTF_8)) {
            p.store(w, null);
        }

        JsonNode idle = status(config);
        assertEquals("stopped", idle.path("daemon").asText());
        assertEquals(0, idle.path("samples_saved").asLong());

        daemon = new Daemon(DeviceConfig.load(config));
        daemonThread = new Thread(() -> {
            try {
                daemon.run();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }, "daemon-under-test");
        daemonThread.start();

        JsonNode first = statusWhenSaved(config, 0, 30_000);
        assertEquals("offline", first.path("mode").asText());
        assertEquals(ProcessHandle.current().pid(), first.path("pid").asLong());
        assertEquals(mock.session().id(), first.path("session").asText());
        assertEquals(0, first.path("samples_in_cloud").asLong());
        assertEquals(first.path("samples_saved").asLong() - first.path("samples_in_cloud").asLong(), first.path("backlog").asLong());
        assertEquals(0, first.path("loss_ranges").asLong());

        JsonNode later = statusWhenSaved(config, first.path("samples_saved").asLong() + 800, 15_000);
        assertEquals(later.path("samples_saved").asLong(), later.path("backlog").asLong());

        daemon.stop();
        daemonThread.join(20_000);
        assertFalse(daemonThread.isAlive());
        JsonNode stopped = status(config);
        assertEquals("stopped", stopped.path("daemon").asText());
        assertEquals(mock.session().id(), stopped.path("session").asText());
        assertTrue(stopped.path("samples_saved").asLong() >= later.path("samples_saved").asLong());
        daemon = null;
    }
}
