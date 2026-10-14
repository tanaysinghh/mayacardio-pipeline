package com.mayacardio.pipeline.device.collector;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Locale;

public final class DiskGuard {

    public interface SpaceProbe {
        long usableBytes(Path dir) throws IOException;
    }

    public static final SpaceProbe REAL = dir -> Files.getFileStore(dir).getUsableSpace();

    private final Path dir;
    private final Path reserve;
    private final long reserveBytes;
    private final long minFree;
    private final long resumeFree;
    private final SpaceProbe probe;

    public DiskGuard(Path dir, long reserveBytes, long minFree, long resumeFree, SpaceProbe probe) {
        this.dir = dir;
        this.reserve = dir.resolve("disk-reserve.bin");
        this.reserveBytes = reserveBytes;
        this.minFree = minFree;
        this.resumeFree = resumeFree;
        this.probe = probe;
    }

    public long free() {
        try {
            return probe.usableBytes(dir);
        } catch (IOException e) {
            return Long.MAX_VALUE;
        }
    }

    public boolean low() {
        return free() < minFree;
    }

    public boolean recovered() {
        return free() >= resumeFree;
    }

    public void ensureReserve() {
        if (reserveBytes <= 0 || Files.exists(reserve) || free() < resumeFree + reserveBytes) {
            return;
        }
        byte[] chunk = new byte[64 * 1024];
        try (OutputStream out = Files.newOutputStream(reserve)) {
            long left = reserveBytes;
            while (left > 0) {
                int n = (int) Math.min(chunk.length, left);
                out.write(chunk, 0, n);
                left -= n;
            }
        } catch (IOException e) {
            try {
                Files.deleteIfExists(reserve);
            } catch (IOException ignored) {
            }
        }
    }

    public boolean releaseReserve() {
        try {
            return Files.deleteIfExists(reserve);
        } catch (IOException e) {
            return false;
        }
    }

    public boolean isDiskFull(SQLException e) {
        String msg = String.valueOf(e.getMessage()).toLowerCase(Locale.ROOT);
        return e.getErrorCode() == 13 || msg.contains("sqlite_full") || msg.contains("disk is full")
                || msg.contains("no space") || low();
    }
}
