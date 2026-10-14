package com.mayacardio.pipeline.cloud.store;

import com.mayacardio.pipeline.core.Signal;

import java.io.ByteArrayOutputStream;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public final class Segment {
    public static final int SIZE = Signal.WINDOW;

    private final long seg;
    private final int channels;
    private final boolean[] present = new boolean[SIZE];
    private final int[][] values;
    private int count;

    public Segment(long seg, int channels) {
        this.seg = seg;
        this.channels = channels;
        this.values = new int[channels][SIZE];
    }

    public long seg() {
        return seg;
    }

    public int channels() {
        return channels;
    }

    public int count() {
        return count;
    }

    public long firstIndex() {
        return seg * SIZE;
    }

    public boolean has(int offset) {
        return present[offset];
    }

    public int value(int offset, int channel) {
        return values[channel][offset];
    }

    public int[] row(int offset) {
        int[] r = new int[channels];
        for (int c = 0; c < channels; c++) {
            r[c] = values[c][offset];
        }
        return r;
    }

    public enum Put { NEW, SAME, CONFLICT }

    public Put put(int offset, int[] row) {
        if (present[offset]) {
            for (int c = 0; c < channels; c++) {
                if (values[c][offset] != row[c]) {
                    return Put.CONFLICT;
                }
            }
            return Put.SAME;
        }
        present[offset] = true;
        for (int c = 0; c < channels; c++) {
            values[c][offset] = row[c];
        }
        count++;
        return Put.NEW;
    }

    public byte[] encode() {
        ByteArrayOutputStream raw = new ByteArrayOutputStream(SIZE * channels * 2 + 64);
        byte[] bitmap = new byte[(SIZE + 7) / 8];
        for (int i = 0; i < SIZE; i++) {
            if (present[i]) {
                bitmap[i >> 3] |= (byte) (1 << (i & 7));
            }
        }
        raw.write(channels);
        raw.writeBytes(bitmap);
        for (int c = 0; c < channels; c++) {
            long prev = 0;
            for (int i = 0; i < SIZE; i++) {
                if (present[i]) {
                    long d = values[c][i] - prev;
                    writeVar(raw, (d << 1) ^ (d >> 63));
                    prev = values[c][i];
                }
            }
        }
        Deflater def = new Deflater(Deflater.BEST_SPEED);
        try {
            def.setInput(raw.toByteArray());
            def.finish();
            ByteArrayOutputStream out = new ByteArrayOutputStream(raw.size() / 2 + 16);
            byte[] buf = new byte[4096];
            while (!def.finished()) {
                int n = def.deflate(buf);
                out.write(buf, 0, n);
            }
            return out.toByteArray();
        } finally {
            def.end();
        }
    }

    public static Segment decode(long seg, byte[] data) {
        Inflater inf = new Inflater();
        byte[] raw;
        try {
            inf.setInput(data);
            ByteArrayOutputStream out = new ByteArrayOutputStream(data.length * 3);
            byte[] buf = new byte[8192];
            while (!inf.finished()) {
                int n = inf.inflate(buf);
                if (n == 0 && (inf.needsInput() || inf.needsDictionary())) {
                    break;
                }
                out.write(buf, 0, n);
            }
            raw = out.toByteArray();
        } catch (DataFormatException e) {
            throw new IllegalStateException("corrupt segment " + seg, e);
        } finally {
            inf.end();
        }
        int channels = raw[0];
        Segment s = new Segment(seg, channels);
        int pos = 1;
        for (int i = 0; i < SIZE; i++) {
            if ((raw[pos + (i >> 3)] & (1 << (i & 7))) != 0) {
                s.present[i] = true;
                s.count++;
            }
        }
        pos += (SIZE + 7) / 8;
        int[] cursor = {pos};
        for (int c = 0; c < channels; c++) {
            long prev = 0;
            for (int i = 0; i < SIZE; i++) {
                if (s.present[i]) {
                    long z = readVar(raw, cursor);
                    long d = (z >>> 1) ^ -(z & 1);
                    prev += d;
                    s.values[c][i] = (int) prev;
                }
            }
        }
        return s;
    }

    private static void writeVar(ByteArrayOutputStream out, long v) {
        while ((v & ~0x7FL) != 0) {
            out.write((int) ((v & 0x7F) | 0x80));
            v >>>= 7;
        }
        out.write((int) v);
    }

    private static long readVar(byte[] b, int[] cursor) {
        long result = 0;
        int shift = 0;
        while (true) {
            int x = b[cursor[0]++] & 0xFF;
            result |= (long) (x & 0x7F) << shift;
            if ((x & 0x80) == 0) {
                return result;
            }
            shift += 7;
        }
    }
}
