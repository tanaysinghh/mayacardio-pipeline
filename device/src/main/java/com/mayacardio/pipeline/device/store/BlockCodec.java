package com.mayacardio.pipeline.device.store;

import java.io.ByteArrayOutputStream;

public final class BlockCodec {

    private BlockCodec() {
    }

    public static byte[] encode(int[][] rows, int channels) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(rows.length * channels * 3 + 8);
        long[] prev = new long[channels];
        for (int[] row : rows) {
            for (int c = 0; c < channels; c++) {
                long d = row[c] - prev[c];
                long z = (d << 1) ^ (d >> 63);
                while ((z & ~0x7FL) != 0) {
                    out.write((int) ((z & 0x7F) | 0x80));
                    z >>>= 7;
                }
                out.write((int) z);
                prev[c] = row[c];
            }
        }
        return out.toByteArray();
    }

    public static int[][] decode(byte[] data, int count, int channels) {
        int[][] rows = new int[count][channels];
        long[] prev = new long[channels];
        int pos = 0;
        for (int k = 0; k < count; k++) {
            for (int c = 0; c < channels; c++) {
                long z = 0;
                int shift = 0;
                while (true) {
                    int b = data[pos++] & 0xFF;
                    z |= (long) (b & 0x7F) << shift;
                    if ((b & 0x80) == 0) {
                        break;
                    }
                    shift += 7;
                }
                long v = prev[c] + ((z >>> 1) ^ -(z & 1));
                rows[k][c] = (int) v;
                prev[c] = v;
            }
        }
        if (pos != data.length) {
            throw new IllegalStateException("block length mismatch");
        }
        return rows;
    }
}
