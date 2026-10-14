package com.mayacardio.pipeline.mock;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.zip.CRC32;

public final class MockSession {
    public static final int MAX_PER_REPLY = 400;
    private static final SecureRandom RANDOM = new SecureRandom();

    private final String id;
    private final Generator generator;
    private final long startNanos;
    private final long startedAtMillis;
    private final double speed;
    private final long totalSamples;
    private final long bufferSamples;
    private final long[] cursor = new long[3];
    private final List<long[]>[] outages;

    @SuppressWarnings("unchecked")
    public MockSession(Generator generator, double speed, long durationSeconds, int bufferSeconds) {
        this.id = newId();
        this.generator = generator;
        this.startNanos = System.nanoTime();
        this.startedAtMillis = System.currentTimeMillis();
        this.speed = speed;
        this.totalSamples = durationSeconds <= 0 ? Long.MAX_VALUE : durationSeconds * Generator.RATE;
        this.bufferSamples = (long) bufferSeconds * Generator.RATE;
        this.outages = new List[3];
        outages[1] = new ArrayList<>();
        outages[2] = new ArrayList<>();
    }

    static String newId() {
        byte[] b = new byte[4];
        RANDOM.nextBytes(b);
        return HexFormat.of().formatHex(b);
    }

    public String id() {
        return id;
    }

    public long startedAtMillis() {
        return startedAtMillis;
    }

    public double speed() {
        return speed;
    }

    public long totalSamples() {
        return totalSamples;
    }

    public Generator generator() {
        return generator;
    }

    public synchronized long head() {
        double elapsed = (System.nanoTime() - startNanos) / 1e9;
        long h = (long) Math.floor(elapsed * Generator.RATE * speed);
        return Math.min(h, totalSamples);
    }

    public synchronized void startOutage(int sensor, double seconds) {
        long from = head();
        long to = from + Math.max(1, Math.round(seconds * Generator.RATE)) - 1;
        outages[sensor].add(new long[]{from, to});
    }

    public synchronized List<long[]> outages(int sensor) {
        List<long[]> copy = new ArrayList<>();
        for (long[] o : outages[sensor]) {
            copy.add(o.clone());
        }
        return copy;
    }

    private long[] outageAt(int sensor, long idx) {
        for (long[] o : outages[sensor]) {
            if (idx >= o[0] && idx <= o[1]) {
                return o;
            }
        }
        return null;
    }

    public synchronized String read(int sensor, Long fromIndex) {
        long head = head();
        long oldest = Math.max(0, head - bufferSamples);
        long pos = fromIndex != null ? Math.max(0, fromIndex) : cursor[sensor];
        StringBuilder lost = new StringBuilder();
        if (pos < oldest) {
            appendLost(lost, pos, oldest - 1, "buffer_overflow");
            pos = oldest;
        }
        while (pos < head) {
            long[] o = outageAt(sensor, pos);
            if (o == null) {
                break;
            }
            long end = Math.min(o[1], head - 1);
            appendLost(lost, pos, end, "sensor_outage");
            pos = end + 1;
        }
        long first = pos;
        StringBuilder data = new StringBuilder(MAX_PER_REPLY * 28);
        data.append('[');
        int count = 0;
        while (pos < head && count < MAX_PER_REPLY && outageAt(sensor, pos) == null) {
            int[] row = generator.sample(sensor, pos);
            if (count > 0) {
                data.append(", ");
            }
            data.append('[');
            for (int c = 0; c < row.length; c++) {
                if (c > 0) {
                    data.append(", ");
                }
                data.append(row[c]);
            }
            data.append(']');
            count++;
            pos++;
        }
        data.append(']');
        cursor[sensor] = pos;
        boolean end = totalSamples != Long.MAX_VALUE && pos >= totalSamples;
        String dataText = data.toString();
        return "{\"session_id\": \"" + id + "\", \"sensor\": " + sensor + ", \"first_index\": " + first
                + ", \"count\": " + count + ", \"lost\": [" + lost + "], \"session_end\": " + end
                + ", \"crc32\": \"" + crcHex(dataText) + "\", \"data\": " + dataText + "}";
    }

    private static void appendLost(StringBuilder lost, long from, long to, String reason) {
        if (lost.length() > 0) {
            lost.append(", ");
        }
        lost.append("{\"from_index\": ").append(from).append(", \"to_index\": ").append(to)
                .append(", \"reason\": \"").append(reason).append("\"}");
    }

    public static String crcHex(String text) {
        CRC32 crc = new CRC32();
        crc.update(text.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return String.format("%08x", crc.getValue());
    }
}
