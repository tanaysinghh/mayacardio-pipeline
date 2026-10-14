package com.mayacardio.pipeline.tools.harness;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class Proc {
    private final String name;
    private final List<String> command;
    private final Path log;
    private final Map<String, String> env;
    private Process process;

    public Proc(String name, List<String> command, Path log, Map<String, String> env) {
        this.name = name;
        this.command = command;
        this.log = log;
        this.env = env;
    }

    public Proc start() throws IOException {
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);
        pb.redirectOutput(ProcessBuilder.Redirect.appendTo(log.toFile()));
        if (env != null) {
            pb.environment().putAll(env);
        }
        process = pb.start();
        return this;
    }

    public String name() {
        return name;
    }

    public long pid() {
        return process == null ? -1 : process.pid();
    }

    public boolean alive() {
        return process != null && process.isAlive();
    }

    public void kill9() throws InterruptedException {
        if (process != null) {
            process.descendants().forEach(ProcessHandle::destroyForcibly);
            process.destroyForcibly();
            process.waitFor(10, TimeUnit.SECONDS);
        }
    }

    public void terminate() throws InterruptedException {
        if (process != null && process.isAlive()) {
            process.descendants().forEach(ProcessHandle::destroy);
            process.destroy();
            if (!process.waitFor(15, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                process.waitFor(5, TimeUnit.SECONDS);
            }
        }
    }

    public int waitFor(long seconds) throws InterruptedException {
        if (!process.waitFor(seconds, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            return -1;
        }
        return process.exitValue();
    }
}
