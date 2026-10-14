package com.mayacardio.pipeline.core;

public final class SampleBlock {
    private final long from;
    private final int length;
    private final int channels;
    private final boolean[] present;
    private final int[][] values;

    public SampleBlock(long from, long to, int channels) {
        this.from = from;
        this.length = (int) Math.max(0, to - from + 1);
        this.channels = channels;
        this.present = new boolean[length];
        this.values = new int[channels][length];
    }

    public long from() {
        return from;
    }

    public long to() {
        return from + length - 1;
    }

    public int length() {
        return length;
    }

    public int channels() {
        return channels;
    }

    public void put(long idx, int[] row) {
        int k = offset(idx);
        if (k < 0) {
            return;
        }
        present[k] = true;
        for (int c = 0; c < channels; c++) {
            values[c][k] = row[c];
        }
    }

    public void put(long idx, int c0, int c1, int c2) {
        int k = offset(idx);
        if (k < 0) {
            return;
        }
        present[k] = true;
        values[0][k] = c0;
        values[1][k] = c1;
        if (channels > 2) {
            values[2][k] = c2;
        }
    }

    public boolean has(long idx) {
        int k = offset(idx);
        return k >= 0 && present[k];
    }

    public int value(long idx, int channel) {
        return values[channel][offset(idx)];
    }

    public int[] row(long idx) {
        int k = offset(idx);
        int[] r = new int[channels];
        for (int c = 0; c < channels; c++) {
            r[c] = values[c][k];
        }
        return r;
    }

    public int presentCount() {
        int n = 0;
        for (boolean p : present) {
            if (p) {
                n++;
            }
        }
        return n;
    }

    public boolean allPresent() {
        for (boolean p : present) {
            if (!p) {
                return false;
            }
        }
        return true;
    }

    private int offset(long idx) {
        long k = idx - from;
        if (k < 0 || k >= length) {
            return -1;
        }
        return (int) k;
    }
}
