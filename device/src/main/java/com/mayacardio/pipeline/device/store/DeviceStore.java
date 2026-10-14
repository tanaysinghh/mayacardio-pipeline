package com.mayacardio.pipeline.device.store;

import com.mayacardio.pipeline.core.LossRange;
import com.mayacardio.pipeline.core.RangeSet;
import com.mayacardio.pipeline.core.SampleBlock;
import com.mayacardio.pipeline.core.SampleRun;
import com.mayacardio.pipeline.core.Signal;
import com.mayacardio.pipeline.device.Clocks;
import org.sqlite.SQLiteConfig;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

public final class DeviceStore implements AutoCloseable {

    public record SessionInfo(long sid, String sessionId, String firstSeen, boolean closed, String closeReason) {
    }

    public record StreamState(long sid, int sensor, long nextIndex, long storedCount, boolean ended) {
    }

    public record Commit(long sid, int sensor, List<LossRange> losses, long dataFirst, int[][] rows,
                         long newNext, boolean ended) {
    }

    public record StoredResult(long window, String mode, String body) {
    }

    private static final String[] SCHEMA = {
            "CREATE TABLE IF NOT EXISTS sessions(sid INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT NOT NULL UNIQUE,"
                    + " first_seen TEXT NOT NULL, closed INTEGER NOT NULL DEFAULT 0, close_reason TEXT, closed_at TEXT)",
            "CREATE TABLE IF NOT EXISTS samples(sid INTEGER NOT NULL, sensor INTEGER NOT NULL, idx INTEGER NOT NULL,"
                    + " v1 INTEGER NOT NULL, v2 INTEGER NOT NULL, v3 INTEGER, PRIMARY KEY(sid, sensor, idx)) WITHOUT ROWID",
            "CREATE TABLE IF NOT EXISTS losses(sid INTEGER NOT NULL, sensor INTEGER NOT NULL, from_index INTEGER NOT NULL,"
                    + " to_index INTEGER, reason TEXT NOT NULL, recorded_at TEXT NOT NULL, synced INTEGER NOT NULL DEFAULT 0,"
                    + " PRIMARY KEY(sid, sensor, from_index))",
            "CREATE TABLE IF NOT EXISTS stream_state(sid INTEGER NOT NULL, sensor INTEGER NOT NULL, next_index INTEGER NOT NULL,"
                    + " stored_count INTEGER NOT NULL, ended INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(sid, sensor))",
            "CREATE TABLE IF NOT EXISTS stored_runs(sid INTEGER NOT NULL, sensor INTEGER NOT NULL, from_index INTEGER NOT NULL,"
                    + " to_index INTEGER NOT NULL, PRIMARY KEY(sid, sensor, from_index))",
            "CREATE TABLE IF NOT EXISTS sync_state(sid INTEGER PRIMARY KEY, cloud_held_1 INTEGER NOT NULL DEFAULT 0,"
                    + " cloud_held_2 INTEGER NOT NULL DEFAULT 0, meta_synced INTEGER NOT NULL DEFAULT 0,"
                    + " cloud_complete INTEGER NOT NULL DEFAULT 0, updated_at TEXT)",
            "CREATE TABLE IF NOT EXISTS results(sid INTEGER NOT NULL, win INTEGER NOT NULL, mode TEXT NOT NULL, body TEXT NOT NULL,"
                    + " computed_at TEXT NOT NULL, synced INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(sid, win, mode))",
            "CREATE TABLE IF NOT EXISTS offline_progress(sid INTEGER PRIMARY KEY, next_window INTEGER NOT NULL)",
            "CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY AUTOINCREMENT, at TEXT NOT NULL, mono_ms INTEGER NOT NULL,"
                    + " kind TEXT NOT NULL, detail TEXT)"
    };

    private final Connection conn;

    private DeviceStore(Connection conn) {
        this.conn = conn;
    }

    public static DeviceStore open(Path db) throws SQLException {
        try {
            Path parent = db.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
        } catch (java.io.IOException e) {
            throw new SQLException("cannot create data dir: " + e.getMessage(), e);
        }
        SQLiteConfig cfg = new SQLiteConfig();
        cfg.setJournalMode(SQLiteConfig.JournalMode.WAL);
        cfg.setSynchronous(SQLiteConfig.SynchronousMode.FULL);
        cfg.setBusyTimeout(15_000);
        cfg.setJournalSizeLimit(64 * 1024 * 1024);
        Connection c = DriverManager.getConnection("jdbc:sqlite:" + db.toAbsolutePath(), cfg.toProperties());
        DeviceStore s = new DeviceStore(c);
        s.init();
        return s;
    }

    private void init() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("PRAGMA synchronous=FULL");
            for (String ddl : SCHEMA) {
                st.execute(ddl);
            }
        }
    }

    public synchronized String pragma(String name) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery("PRAGMA " + name)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    private interface Tx<T> {
        T run() throws SQLException;
    }

    private <T> T tx(Tx<T> body) throws SQLException {
        conn.setAutoCommit(false);
        try {
            T out = body.run();
            conn.commit();
            return out;
        } catch (SQLException | RuntimeException e) {
            try {
                conn.rollback();
            } catch (SQLException ignored) {
            }
            throw e;
        } finally {
            try {
                conn.setAutoCommit(true);
            } catch (SQLException ignored) {
            }
        }
    }

    public synchronized SessionInfo currentSession() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT sid, session_id, first_seen, closed, close_reason FROM sessions WHERE closed = 0 ORDER BY sid DESC LIMIT 1");
             ResultSet rs = ps.executeQuery()) {
            return rs.next() ? sessionRow(rs) : null;
        }
    }

    public synchronized SessionInfo latestSession() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT sid, session_id, first_seen, closed, close_reason FROM sessions ORDER BY sid DESC LIMIT 1");
             ResultSet rs = ps.executeQuery()) {
            return rs.next() ? sessionRow(rs) : null;
        }
    }

    public synchronized SessionInfo session(String sessionId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT sid, session_id, first_seen, closed, close_reason FROM sessions WHERE session_id = ?")) {
            ps.setString(1, sessionId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? sessionRow(rs) : null;
            }
        }
    }

    public synchronized List<SessionInfo> sessions() throws SQLException {
        List<SessionInfo> out = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT sid, session_id, first_seen, closed, close_reason FROM sessions ORDER BY sid");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                out.add(sessionRow(rs));
            }
        }
        return out;
    }

    private static SessionInfo sessionRow(ResultSet rs) throws SQLException {
        return new SessionInfo(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getInt(4) != 0, rs.getString(5));
    }

    public synchronized long createSession(String sessionId) throws SQLException {
        return tx(() -> {
            long sid;
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO sessions(session_id, first_seen) VALUES(?, ?)", Statement.RETURN_GENERATED_KEYS)) {
                ps.setString(1, sessionId);
                ps.setString(2, Clocks.wallIso());
                ps.executeUpdate();
                try (ResultSet k = ps.getGeneratedKeys()) {
                    k.next();
                    sid = k.getLong(1);
                }
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO stream_state(sid, sensor, next_index, stored_count) VALUES(?, ?, 0, 0)")) {
                for (int s = 1; s <= Signal.SENSOR_COUNT; s++) {
                    ps.setLong(1, sid);
                    ps.setInt(2, s);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO sync_state(sid, updated_at) VALUES(?, ?)")) {
                ps.setLong(1, sid);
                ps.setString(2, Clocks.wallIso());
                ps.executeUpdate();
            }
            return sid;
        });
    }

    public synchronized void closeSession(long sid, String reason, List<LossRange> tail) throws SQLException {
        tx(() -> {
            insertLosses(sid, tail);
            try (PreparedStatement ps = conn.prepareStatement("UPDATE stream_state SET ended = 1 WHERE sid = ?")) {
                ps.setLong(1, sid);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE sessions SET closed = 1, close_reason = ?, closed_at = ? WHERE sid = ?")) {
                ps.setString(1, reason);
                ps.setString(2, Clocks.wallIso());
                ps.setLong(3, sid);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement("UPDATE sync_state SET meta_synced = 0, cloud_complete = 0 WHERE sid = ?")) {
                ps.setLong(1, sid);
                ps.executeUpdate();
            }
            return null;
        });
    }

    public synchronized long commit(Commit c) throws SQLException {
        return tx(() -> {
            insertLosses(c.sid(), c.losses());
            long inserted = 0;
            int[][] rows = c.rows();
            if (rows.length > 0) {
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT OR IGNORE INTO samples(sid, sensor, idx, v1, v2, v3) VALUES(?, ?, ?, ?, ?, ?)")) {
                    for (int k = 0; k < rows.length; k++) {
                        int[] r = rows[k];
                        ps.setLong(1, c.sid());
                        ps.setInt(2, c.sensor());
                        ps.setLong(3, c.dataFirst() + k);
                        ps.setInt(4, r[0]);
                        ps.setInt(5, r[1]);
                        if (r.length > 2) {
                            ps.setInt(6, r[2]);
                        } else {
                            ps.setNull(6, Types.INTEGER);
                        }
                        ps.addBatch();
                    }
                    for (int n : ps.executeBatch()) {
                        if (n > 0) {
                            inserted += n;
                        }
                    }
                }
                long last = c.dataFirst() + rows.length - 1;
                int extended;
                try (PreparedStatement ps = conn.prepareStatement(
                        "UPDATE stored_runs SET to_index = ? WHERE sid = ? AND sensor = ? AND to_index = ?")) {
                    ps.setLong(1, last);
                    ps.setLong(2, c.sid());
                    ps.setInt(3, c.sensor());
                    ps.setLong(4, c.dataFirst() - 1);
                    extended = ps.executeUpdate();
                }
                if (extended == 0) {
                    try (PreparedStatement ps = conn.prepareStatement(
                            "INSERT OR REPLACE INTO stored_runs(sid, sensor, from_index, to_index) VALUES(?, ?, ?, ?)")) {
                        ps.setLong(1, c.sid());
                        ps.setInt(2, c.sensor());
                        ps.setLong(3, c.dataFirst());
                        ps.setLong(4, last);
                        ps.executeUpdate();
                    }
                }
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE stream_state SET next_index = MAX(next_index, ?), stored_count = stored_count + ?, ended = ?"
                            + " WHERE sid = ? AND sensor = ?")) {
                ps.setLong(1, c.newNext());
                ps.setLong(2, inserted);
                ps.setInt(3, c.ended() ? 1 : 0);
                ps.setLong(4, c.sid());
                ps.setInt(5, c.sensor());
                ps.executeUpdate();
            }
            return inserted;
        });
    }

    private void insertLosses(long sid, List<LossRange> losses) throws SQLException {
        if (losses == null || losses.isEmpty()) {
            return;
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT OR IGNORE INTO losses(sid, sensor, from_index, to_index, reason, recorded_at) VALUES(?, ?, ?, ?, ?, ?)")) {
            String now = Clocks.wallIso();
            for (LossRange l : losses) {
                ps.setLong(1, sid);
                ps.setInt(2, l.sensor());
                ps.setLong(3, l.fromIndex());
                if (l.toIndex() == null) {
                    ps.setNull(4, Types.INTEGER);
                } else {
                    ps.setLong(4, l.toIndex());
                }
                ps.setString(5, l.reason());
                ps.setString(6, now);
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    public synchronized StreamState stream(long sid, int sensor) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT next_index, stored_count, ended FROM stream_state WHERE sid = ? AND sensor = ?")) {
            ps.setLong(1, sid);
            ps.setInt(2, sensor);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return new StreamState(sid, sensor, rs.getLong(1), rs.getLong(2), rs.getInt(3) != 0);
            }
        }
    }

    public synchronized RangeSet storedRuns(long sid, int sensor) throws SQLException {
        RangeSet set = new RangeSet();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT from_index, to_index FROM stored_runs WHERE sid = ? AND sensor = ? ORDER BY from_index")) {
            ps.setLong(1, sid);
            ps.setInt(2, sensor);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    set.add(rs.getLong(1), rs.getLong(2));
                }
            }
        }
        return set;
    }

    public synchronized List<LossRange> losses(long sid) throws SQLException {
        return lossQuery("SELECT sensor, from_index, to_index, reason FROM losses WHERE sid = ? ORDER BY sensor, from_index", sid);
    }

    public synchronized List<LossRange> unsyncedLosses(long sid) throws SQLException {
        return lossQuery("SELECT sensor, from_index, to_index, reason FROM losses WHERE sid = ? AND synced = 0 ORDER BY sensor, from_index", sid);
    }

    private List<LossRange> lossQuery(String sql, long sid) throws SQLException {
        List<LossRange> out = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, sid);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long to = rs.getLong(3);
                    Long toIdx = rs.wasNull() ? null : to;
                    out.add(new LossRange(rs.getInt(1), rs.getLong(2), toIdx, rs.getString(4)));
                }
            }
        }
        return out;
    }

    public synchronized void markLossesSynced(long sid, List<LossRange> losses) throws SQLException {
        tx(() -> {
            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE losses SET synced = 1 WHERE sid = ? AND sensor = ? AND from_index = ?")) {
                for (LossRange l : losses) {
                    ps.setLong(1, sid);
                    ps.setInt(2, l.sensor());
                    ps.setLong(3, l.fromIndex());
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            return null;
        });
    }

    public synchronized SampleBlock block(long sid, int sensor, long from, long to) throws SQLException {
        SampleBlock block = new SampleBlock(from, to, Signal.channels(sensor));
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT idx, v1, v2, v3 FROM samples WHERE sid = ? AND sensor = ? AND idx BETWEEN ? AND ?")) {
            ps.setLong(1, sid);
            ps.setInt(2, sensor);
            ps.setLong(3, from);
            ps.setLong(4, to);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    block.put(rs.getLong(1), rs.getInt(2), rs.getInt(3), rs.getInt(4));
                }
            }
        }
        return block;
    }

    public synchronized List<SampleRun> runs(long sid, int sensor, long from, long to) throws SQLException {
        List<SampleRun> out = new ArrayList<>();
        int channels = Signal.channels(sensor);
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT idx, v1, v2, v3 FROM samples WHERE sid = ? AND sensor = ? AND idx BETWEEN ? AND ? ORDER BY idx")) {
            ps.setLong(1, sid);
            ps.setInt(2, sensor);
            ps.setLong(3, from);
            ps.setLong(4, to);
            try (ResultSet rs = ps.executeQuery()) {
                List<int[]> cur = new ArrayList<>();
                long curFirst = -1;
                long prev = -2;
                while (rs.next()) {
                    long idx = rs.getLong(1);
                    if (idx != prev + 1 && !cur.isEmpty()) {
                        out.add(new SampleRun(sensor, curFirst, cur.toArray(new int[0][])));
                        cur = new ArrayList<>();
                    }
                    if (cur.isEmpty()) {
                        curFirst = idx;
                    }
                    int[] row = channels == 3 ? new int[]{rs.getInt(2), rs.getInt(3), rs.getInt(4)}
                            : new int[]{rs.getInt(2), rs.getInt(3)};
                    cur.add(row);
                    prev = idx;
                }
                if (!cur.isEmpty()) {
                    out.add(new SampleRun(sensor, curFirst, cur.toArray(new int[0][])));
                }
            }
        }
        return out;
    }

    public interface SampleConsumer {
        void accept(int sensor, long idx, int[] values) throws java.io.IOException;
    }

    public synchronized void forEachSample(long sid, int sensor, SampleConsumer consumer) throws SQLException, java.io.IOException {
        int channels = Signal.channels(sensor);
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT idx, v1, v2, v3 FROM samples WHERE sid = ? AND sensor = ? ORDER BY idx")) {
            ps.setFetchSize(4000);
            ps.setLong(1, sid);
            ps.setInt(2, sensor);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    int[] row = channels == 3 ? new int[]{rs.getInt(2), rs.getInt(3), rs.getInt(4)}
                            : new int[]{rs.getInt(2), rs.getInt(3)};
                    consumer.accept(sensor, rs.getLong(1), row);
                }
            }
        }
    }

    public synchronized void insertResults(long sid, List<StoredResult> results, Long nextOfflineWindow) throws SQLException {
        tx(() -> {
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT OR REPLACE INTO results(sid, win, mode, body, computed_at, synced) VALUES(?, ?, ?, ?, ?, 0)")) {
                String now = Clocks.wallIso();
                for (StoredResult r : results) {
                    ps.setLong(1, sid);
                    ps.setLong(2, r.window());
                    ps.setString(3, r.mode());
                    ps.setString(4, r.body());
                    ps.setString(5, now);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            if (nextOfflineWindow != null) {
                setOfflineNextInternal(sid, nextOfflineWindow);
            }
            return null;
        });
    }

    public synchronized List<StoredResult> results(long sid, boolean unsyncedOnly, int limit) throws SQLException {
        List<StoredResult> out = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT win, mode, body FROM results WHERE sid = ?" + (unsyncedOnly ? " AND synced = 0" : "")
                        + " ORDER BY win, mode LIMIT ?")) {
            ps.setLong(1, sid);
            ps.setInt(2, limit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new StoredResult(rs.getLong(1), rs.getString(2), rs.getString(3)));
                }
            }
        }
        return out;
    }

    public synchronized void markResultsSynced(long sid, List<StoredResult> results) throws SQLException {
        tx(() -> {
            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE results SET synced = 1 WHERE sid = ? AND win = ? AND mode = ?")) {
                for (StoredResult r : results) {
                    ps.setLong(1, sid);
                    ps.setLong(2, r.window());
                    ps.setString(3, r.mode());
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            return null;
        });
    }

    public synchronized Long offlineNext(long sid) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT next_window FROM offline_progress WHERE sid = ?")) {
            ps.setLong(1, sid);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : null;
            }
        }
    }

    public synchronized void setOfflineNext(long sid, long next) throws SQLException {
        tx(() -> {
            setOfflineNextInternal(sid, next);
            return null;
        });
    }

    public synchronized void clearOfflineNext(long sid) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("DELETE FROM offline_progress WHERE sid = ?")) {
            ps.setLong(1, sid);
            ps.executeUpdate();
        }
    }

    private void setOfflineNextInternal(long sid, long next) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO offline_progress(sid, next_window) VALUES(?, ?) ON CONFLICT(sid) DO UPDATE SET next_window = excluded.next_window")) {
            ps.setLong(1, sid);
            ps.setLong(2, next);
            ps.executeUpdate();
        }
    }

    public synchronized long[] syncState(long sid) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT cloud_held_1, cloud_held_2, meta_synced, cloud_complete FROM sync_state WHERE sid = ?")) {
            ps.setLong(1, sid);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return new long[]{0, 0, 0, 0};
                }
                return new long[]{rs.getLong(1), rs.getLong(2), rs.getLong(3), rs.getLong(4)};
            }
        }
    }

    public synchronized void updateSync(long sid, Long held1, Long held2, Boolean metaSynced, Boolean complete) throws SQLException {
        tx(() -> {
            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE sync_state SET cloud_held_1 = COALESCE(?, cloud_held_1), cloud_held_2 = COALESCE(?, cloud_held_2),"
                            + " meta_synced = COALESCE(?, meta_synced), cloud_complete = COALESCE(?, cloud_complete), updated_at = ? WHERE sid = ?")) {
                setNullable(ps, 1, held1);
                setNullable(ps, 2, held2);
                setNullable(ps, 3, metaSynced == null ? null : (metaSynced ? 1L : 0L));
                setNullable(ps, 4, complete == null ? null : (complete ? 1L : 0L));
                ps.setString(5, Clocks.wallIso());
                ps.setLong(6, sid);
                ps.executeUpdate();
            }
            return null;
        });
    }

    private static void setNullable(PreparedStatement ps, int i, Long v) throws SQLException {
        if (v == null) {
            ps.setNull(i, Types.INTEGER);
        } else {
            ps.setLong(i, v);
        }
    }

    public synchronized void event(String kind, String detail) {
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO events(at, mono_ms, kind, detail) VALUES(?, ?, ?, ?)")) {
            ps.setString(1, Clocks.wallIso());
            ps.setLong(2, Clocks.monoMs());
            ps.setString(3, kind);
            ps.setString(4, detail);
            ps.executeUpdate();
        } catch (SQLException ignored) {
        }
    }

    public synchronized long lossCount(long sid) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM losses WHERE sid = ?")) {
            ps.setLong(1, sid);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    public synchronized long countSamples(long sid, int sensor) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM samples WHERE sid = ? AND sensor = ?")) {
            ps.setLong(1, sid);
            ps.setInt(2, sensor);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    public synchronized void checkpoint() {
        try (Statement st = conn.createStatement()) {
            st.execute("PRAGMA wal_checkpoint(PASSIVE)");
        } catch (SQLException ignored) {
        }
    }

    @Override
    public synchronized void close() {
        try {
            conn.close();
        } catch (SQLException ignored) {
        }
    }
}
