package com.mayacardio.pipeline.mock;

import java.nio.file.Path;

public final class MockMain {

    private MockMain() {
    }

    public static void main(String[] args) throws Exception {
        String mode = "signal";
        long seed = 7;
        long duration = 3600;
        int port = 7100;
        Integer controlPort = null;
        double speed = 1.0;
        int buffer = 10;
        Path gt = Path.of("mock-ground-truth");
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--mode" -> mode = args[++i];
                case "--seed" -> seed = Long.parseLong(args[++i]);
                case "--duration" -> duration = Long.parseLong(args[++i]);
                case "--port" -> port = Integer.parseInt(args[++i]);
                case "--control-port" -> controlPort = Integer.parseInt(args[++i]);
                case "--speed" -> speed = Double.parseDouble(args[++i]);
                case "--buffer-seconds" -> buffer = Integer.parseInt(args[++i]);
                case "--ground-truth-dir" -> gt = Path.of(args[++i]);
                case "--help", "-h" -> {
                    usage();
                    return;
                }
                default -> {
                    System.err.println("unknown argument: " + args[i]);
                    usage();
                    System.exit(2);
                }
            }
        }
        int cport = controlPort == null ? port + 1 : controlPort;
        MockServer server = new MockServer(new Generator(mode, seed), speed, duration, buffer, port, cport, gt);
        server.start();
        System.out.println("biosignal-mock STAND-IN listening on " + server.port() + " control " + server.controlPort()
                + " session " + server.session().id() + " mode " + mode + " seed " + seed + " speed " + speed);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                server.close();
            } catch (Exception ignored) {
            }
        }));
        Thread.currentThread().join();
    }

    private static void usage() {
        System.out.println("""
                biosignal-mock STAND-IN (not the official mock)
                  --mode signal|ramp  --seed N  --duration SECONDS (0 = unlimited)  --port N
                  --control-port N (default port+1, -1 disables)  --speed FACTOR  --buffer-seconds N
                  --ground-truth-dir DIR
                control port accepts lines: stall SECONDS | corrupt N | duplicate N | outage SENSOR SECONDS | speed FACTOR | restart | status""");
    }
}
