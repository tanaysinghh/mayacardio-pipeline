package com.mayacardio.pipeline.core;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public final class BatchCodec {
    public static final int MAGIC = 0x4D434231;
    public static final int MAX_RUNS = 100_000;
    public static final int MAX_SAMPLES = 2_000_000;

    public record Batch(String sessionId, long capturedAtMillis, List<SampleRun> runs) {
        public long sampleCount() {
            long n = 0;
            for (SampleRun r : runs) {
                n += r.count();
            }
            return n;
        }
    }

    private BatchCodec() {
    }

    public static byte[] encode(Batch batch) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(8192);
            try (GZIPOutputStream gz = new GZIPOutputStream(bytes, 16384);
                 DataOutputStream out = new DataOutputStream(gz)) {
                out.writeInt(MAGIC);
                out.writeUTF(batch.sessionId());
                out.writeLong(batch.capturedAtMillis());
                out.writeInt(batch.runs().size());
                for (SampleRun run : batch.runs()) {
                    int channels = Signal.channels(run.sensor());
                    out.writeByte(run.sensor());
                    out.writeByte(channels);
                    out.writeLong(run.firstIndex());
                    out.writeInt(run.count());
                    int[] prev = new int[channels];
                    for (int[] row : run.rows()) {
                        for (int c = 0; c < channels; c++) {
                            writeVarLong(out, zigzag((long) row[c] - prev[c]));
                            prev[c] = row[c];
                        }
                    }
                }
            }
            return bytes.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static Batch decode(byte[] body) throws IOException {
        return decode(new ByteArrayInputStream(body));
    }

    public static Batch decode(InputStream raw) throws IOException {
        try (DataInputStream in = new DataInputStream(new GZIPInputStream(raw, 16384))) {
            if (in.readInt() != MAGIC) {
                throw new IOException("bad magic");
            }
            String session = in.readUTF();
            if (session.isEmpty() || session.length() > 128) {
                throw new IOException("bad session id");
            }
            long captured = in.readLong();
            int runCount = in.readInt();
            if (runCount < 0 || runCount > MAX_RUNS) {
                throw new IOException("bad run count");
            }
            List<SampleRun> runs = new ArrayList<>(runCount);
            long total = 0;
            for (int r = 0; r < runCount; r++) {
                int sensor = in.readUnsignedByte();
                int channels = in.readUnsignedByte();
                if (!Signal.validSensor(sensor) || channels != Signal.channels(sensor)) {
                    throw new IOException("bad sensor/channels");
                }
                long first = in.readLong();
                int count = in.readInt();
                total += count;
                if (first < 0 || count < 0 || total > MAX_SAMPLES) {
                    throw new IOException("bad run bounds");
                }
                int[][] rows = new int[count][channels];
                long[] prev = new long[channels];
                for (int k = 0; k < count; k++) {
                    for (int c = 0; c < channels; c++) {
                        long v = prev[c] + unzigzag(readVarLong(in));
                        if (!Signal.validValue(v)) {
                            throw new IOException("value out of 24-bit range");
                        }
                        rows[k][c] = (int) v;
                        prev[c] = v;
                    }
                }
                runs.add(new SampleRun(sensor, first, rows));
            }
            return new Batch(session, captured, runs);
        }
    }

    static long zigzag(long v) {
        return (v << 1) ^ (v >> 63);
    }

    static long unzigzag(long v) {
        return (v >>> 1) ^ -(v & 1);
    }

    static void writeVarLong(OutputStream out, long v) throws IOException {
        while ((v & ~0x7FL) != 0) {
            out.write((int) ((v & 0x7F) | 0x80));
            v >>>= 7;
        }
        out.write((int) v);
    }

    static long readVarLong(InputStream in) throws IOException {
        long result = 0;
        int shift = 0;
        while (shift < 64) {
            int b = in.read();
            if (b < 0) {
                throw new EOFException();
            }
            result |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
        }
        throw new IOException("varint too long");
    }
}
