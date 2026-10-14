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
            "CREATE TABLE IF NOT EXISTS sample_blocks(sid INTEGER NOT NULL, sensor INTEGER NOT NULL, first_idx INTEGER NOT NULL,"
                    + " count INTEGER NOT NULL, data BLOB NOT NULL, UNIQUE(sid, sensor, first_idx))",
            "CREATE TABLE IF NOT EXISTS losses(sid INTEGER NOT NULL, sensor INTEGER NOT NULL, from_index INTEGER NOT NULL,"
                    + " to_index INTEGER, reason TEXT NOT NULL, recorded_at TEXT NOT NULL, synced INTEGER NOT NULL DEFAULT 0,"
                    + " PRIMARY KEY(sid, sensor, from_index))",
            "CREATE TABLE IF NOT EXISTS stream_state(sid INTEGER NOT NULL, sensor INTEGER NOT NULL, next_index INTEGER NOT NULL,"
                    + " stored_count INTEGER NOT NULL, ended INTEGER NOT NULL DEFAULT 0, run_from INTEGER, run_to INTEGER,"
                    + " PRIMARY KEY(sid, sensor))",
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

    public static final int DEFAULT_PAGE_SIZE = 1024;

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
        if (!Files.exists(db) || db.toFile().length() == 0) {
            try (Connection init = DriverManager.getConnection("jdbc:sqlite:" + db.toAbsolutePath());
                 Statement st = init.createStatement()) {
                st.execute("PRAGMA page_size=" + pageSize());
                st.execute("PRAGMA journal_mode=WAL");
            }
        }
        Connection c = DriverManager.getConnection("jdbc:sqlite:" + db.toAbsolutePath(), cfg.toProperties());
        DeviceStore s = new DeviceStore(c);
        s.init();
        return s;
    }

    static int pageSize() {
        String v = System.getProperty("pipeline.db.page.size", System.getenv("PIPELINE_DB_PAGE_SIZE"));
        return v == null || v.isBlank() ? DEFAULT_PAGE_SIZE : Integer.parseInt(v.trim());
    }

    private void init() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("PRAGMA synchronous=FULL");
            for (String ddl : SCHEMA) {
                st.execute(ddl);
            }
            boolean hasRun = false;
            try (ResultSet rs = st.executeQuery("PRAGMA table_info(stream_state)")) {
                while (rs.next()) {
                    hasRun |= "run_from".equals(rs.getString("name"));
                }
            }
            if (!hasRun) {
                st.execute("ALTER TABLE stream_state ADD COLUMN run_from INTEGER");
                st.execute("ALTER TABLE stream_state ADD COLUMN run_to INTEGER");
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
            long committedNext = 0;
            Long runFrom = null;
            Long runTo = null;
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT next_index, run_from, run_to FROM stream_state WHERE sid = ? AND sensor = ?")) {
                ps.setLong(1, c.sid());
                ps.setInt(2, c.sensor());
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        committedNext = rs.getLong(1);
                        long f = rs.getLong(2);
                        runFrom = rs.wasNull() ? null : f;
                        long t = rs.getLong(3);
                        runTo = rs.wasNull() ? null : t;
                    }
                }
            }
            int skip = (int) Math.max(0, Math.min(c.rows().length, committedNext - c.dataFirst()));
            int[][] rows = skip == 0 ? c.rows() : java.util.Arrays.copyOfRange(c.rows(), skip, c.rows().length);
            long first = c.dataFirst() + skip;
            if (rows.length > 0) {
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO sample_blocks(sid, sensor, first_idx, count, data) VALUES(?, ?, ?, ?, ?)")) {
                    ps.setLong(1, c.sid());
                    ps.setInt(2, c.sensor());
                    ps.setLong(3, first);
                    ps.setInt(4, rows.length);
                    ps.setBytes(5, BlockCodec.encode(rows, Signal.channels(c.sensor())));
                    ps.executeUpdate();
                }
                inserted = rows.length;
                long last = first + rows.length - 1;
                if (runTo != null && runTo == first - 1) {
                    runTo = last;
                } else {
                    if (runFrom != null) {
                        try (PreparedStatement ps = conn.prepareStatement(
                                "INSERT OR REPLACE INTO stored_runs(sid, sensor, from_index, to_index) VALUES(?, ?, ?, ?)")) {
                            ps.setLong(1, c.sid());
                            ps.setInt(2, c.sensor());
                            ps.setLong(3, runFrom);
                            ps.setLong(4, runTo);
                            ps.executeUpdate();
                        }
                    }
                    runFrom = first;
                    runTo = last;
                }
                try (PreparedStatement ps = conn.prepareStatement(
                        "UPDATE stream_state SET run_from = ?, run_to = ? WHERE sid = ? AND sensor = ?")) {
                    ps.setLong(1, runFrom);
                    ps.setLong(2, runTo);
                    ps.setLong(3, c.sid());
                    ps.setInt(4, c.sensor());
                    ps.executeUpdate();
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
                "SELECT run_from, run_to FROM stream_state WHERE sid = ? AND sensor = ? AND run_from IS NOT NULL")) {
            ps.setLong(1, sid);
            ps.setInt(2, sensor);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    set.add(rs.getLong(1), rs.getLong(2));
                }
            }
        }
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

    private interface BlockVisitor {
        void visit(long first, int[][] rows) throws java.io.IOException;
    }

    private void blocksOverlapping(long sid, int sensor, long from, long to, BlockVisitor v) throws SQLException, java.io.IOException {
        int channels = Signal.channels(sensor);
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT first_idx, count, data FROM sample_blocks WHERE sid = ? AND sensor = ? AND first_idx <= ?"
                        + " AND first_idx >= (SELECT COALESCE(MAX(first_idx), ?) FROM sample_blocks WHERE sid = ? AND sensor = ? AND first_idx <= ?)"
                        + " ORDER BY first_idx")) {
            ps.setFetchSize(64);
            ps.setLong(1, sid);
            ps.setInt(2, sensor);
            ps.setLong(3, to);
            ps.setLong(4, from);
            ps.setLong(5, sid);
            ps.setInt(6, sensor);
            ps.setLong(7, from);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long first = rs.getLong(1);
                    int count = rs.getInt(2);
                    if (first + count - 1 < from) {
                        continue;
                    }
                    v.visit(first, BlockCodec.decode(rs.getBytes(3), count, channels));
                }
            }
        }
    }

    public synchronized SampleBlock block(long sid, int sensor, long from, long to) throws SQLException {
        SampleBlock block = new SampleBlock(from, to, Signal.channels(sensor));
        try {
            blocksOverlapping(sid, sensor, from, to, (first, rows) -> {
                long a = Math.max(first, from);
                long b = Math.min(first + rows.length - 1, to);
                for (long i = a; i <= b; i++) {
                    block.put(i, rows[(int) (i - first)]);
                }
            });
        } catch (java.io.IOException e) {
            throw new SQLException(e);
        }
        return block;
    }

    public synchronized List<SampleRun> runs(long sid, int sensor, long from, long to) throws SQLException {
        List<SampleRun> out = new ArrayList<>();
        List<int[]> cur = new ArrayList<>();
        long[] curFirst = {-1};
        long[] prev = {-2};
        try {
            blocksOverlapping(sid, sensor, from, to, (first, rows) -> {
                long a = Math.max(first, from);
                long b = Math.min(first + rows.length - 1, to);
                for (long i = a; i <= b; i++) {
                    if (i != prev[0] + 1 && !cur.isEmpty()) {
                        out.add(new SampleRun(sensor, curFirst[0], cur.toArray(new int[0][])));
                        cur.clear();
                    }
                    if (cur.isEmpty()) {
                        curFirst[0] = i;
                    }
                    cur.add(rows[(int) (i - first)]);
                    prev[0] = i;
                }
            });
        } catch (java.io.IOException e) {
            throw new SQLException(e);
        }
        if (!cur.isEmpty()) {
            out.add(new SampleRun(sensor, curFirst[0], cur.toArray(new int[0][])));
        }
        return out;
    }

    public interface SampleConsumer {
        void accept(int sensor, long idx, int[] values) throws java.io.IOException;
    }

    public synchronized void forEachSample(long sid, int sensor, SampleConsumer consumer) throws SQLException, java.io.IOException {
        blocksOverlapping(sid, sensor, 0, Long.MAX_VALUE, (first, rows) -> {
            for (int k = 0; k < rows.length; k++) {
                consumer.accept(sensor, first + k, rows[k]);
            }
        });
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
        try (PreparedStatement ps = conn.prepareStatement("SELECT COALESCE(SUM(count), 0) FROM sample_blocks WHERE sid = ? AND sensor = ?")) {
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
