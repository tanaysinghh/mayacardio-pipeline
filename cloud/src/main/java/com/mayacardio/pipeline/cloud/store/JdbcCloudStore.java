package com.mayacardio.pipeline.cloud.store;

import com.mayacardio.pipeline.core.LossRange;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public final class JdbcCloudStore implements CloudStore {

    public enum Dialect {
        SQLITE("BLOB", "INTEGER"),
        POSTGRES("BYTEA", "BIGINT");

        final String blob;
        final String bigint;

        Dialect(String blob, String bigint) {
            this.blob = blob;
            this.bigint = bigint;
        }
    }

    private final DataSource ds;
    private final Dialect dialect;

    public JdbcCloudStore(DataSource ds, Dialect dialect) {
        this.ds = ds;
        this.dialect = dialect;
    }

    @Override
    public String backend() {
        return dialect.name().toLowerCase();
    }

    @FunctionalInterface
    private interface Work<T> {
        T run(Connection c) throws SQLException;
    }

    private <T> T withConn(boolean tx, Work<T> work) {
        try (Connection c = ds.getConnection()) {
            if (!tx) {
                return work.run(c);
            }
            boolean auto = c.getAutoCommit();
            c.setAutoCommit(false);
            try {
                T out = work.run(c);
                c.commit();
                return out;
            } catch (SQLException | RuntimeException e) {
                c.rollback();
                throw e;
            } finally {
                c.setAutoCommit(auto);
            }
        } catch (SQLException e) {
            throw new StoreException(e);
        }
    }

    public static final class StoreException extends RuntimeException {
        public StoreException(SQLException e) {
            super(e.getMessage(), e);
        }
    }

    @Override
    public void init() {
        String b = dialect.bigint;
        String[] ddl = {
                "CREATE TABLE IF NOT EXISTS cloud_sessions(session_id TEXT PRIMARY KEY, created_at TEXT NOT NULL,"
                        + " closed INTEGER NOT NULL DEFAULT 0, final_next_1 " + b + ", final_next_2 " + b + ", close_reason TEXT)",
                "CREATE TABLE IF NOT EXISTS cloud_segments(session_id TEXT NOT NULL, sensor INTEGER NOT NULL, seg " + b + " NOT NULL,"
                        + " present_count INTEGER NOT NULL, data " + dialect.blob + " NOT NULL, PRIMARY KEY(session_id, sensor, seg))",
                "CREATE TABLE IF NOT EXISTS cloud_losses(session_id TEXT NOT NULL, sensor INTEGER NOT NULL, from_index " + b + " NOT NULL,"
                        + " to_index " + b + ", reason TEXT NOT NULL, PRIMARY KEY(session_id, sensor, from_index))",
                "CREATE TABLE IF NOT EXISTS cloud_results(session_id TEXT NOT NULL, win " + b + " NOT NULL, mode TEXT NOT NULL,"
                        + " body TEXT NOT NULL, computed_at TEXT NOT NULL, PRIMARY KEY(session_id, win, mode))"
        };
        withConn(false, c -> {
            try (Statement st = c.createStatement()) {
                for (String s : ddl) {
                    st.execute(s);
                }
            }
            return null;
        });
    }

    @Override
    public void ensureSession(String sessionId, String createdAt) {
        withConn(false, c -> {
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO cloud_sessions(session_id, created_at) VALUES(?, ?) ON CONFLICT (session_id) DO NOTHING")) {
                ps.setString(1, sessionId);
                ps.setString(2, createdAt);
                ps.executeUpdate();
            }
            return null;
        });
    }

    @Override
    public List<SessionMeta> sessions() {
        return withConn(false, c -> {
            List<SessionMeta> out = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT session_id, created_at, closed, final_next_1, final_next_2, close_reason FROM cloud_sessions ORDER BY created_at");
                 ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(metaRow(rs));
                }
            }
            return out;
        });
    }

    @Override
    public SessionMeta meta(String sessionId) {
        return withConn(false, c -> {
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT session_id, created_at, closed, final_next_1, final_next_2, close_reason FROM cloud_sessions WHERE session_id = ?")) {
                ps.setString(1, sessionId);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? metaRow(rs) : null;
                }
            }
        });
    }

    private static SessionMeta metaRow(ResultSet rs) throws SQLException {
        long n1 = rs.getLong(4);
        Long f1 = rs.wasNull() ? null : n1;
        long n2 = rs.getLong(5);
        Long f2 = rs.wasNull() ? null : n2;
        return new SessionMeta(rs.getString(1), rs.getString(2), rs.getInt(3) != 0, f1, f2, rs.getString(6));
    }

    @Override
    public void setMeta(String sessionId, boolean closed, Long finalNext1, Long finalNext2, String closeReason) {
        withConn(false, c -> {
            try (PreparedStatement ps = c.prepareStatement(
                    "UPDATE cloud_sessions SET closed = ?, final_next_1 = ?, final_next_2 = ?, close_reason = ? WHERE session_id = ?")) {
                ps.setInt(1, closed ? 1 : 0);
                setLong(ps, 2, finalNext1);
                setLong(ps, 3, finalNext2);
                ps.setString(4, closeReason);
                ps.setString(5, sessionId);
                ps.executeUpdate();
            }
            return null;
        });
    }

    private static void setLong(PreparedStatement ps, int i, Long v) throws SQLException {
        if (v == null) {
            ps.setNull(i, Types.BIGINT);
        } else {
            ps.setLong(i, v);
        }
    }

    @Override
    public Map<Long, Segment> loadSegments(String sessionId, int sensor, Collection<Long> segs) {
        Map<Long, Segment> out = new HashMap<>();
        if (segs.isEmpty()) {
            return out;
        }
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        for (long s : segs) {
            min = Math.min(min, s);
            max = Math.max(max, s);
        }
        Set<Long> wanted = new HashSet<>(segs);
        long lo = min;
        long hi = max;
        return withConn(false, c -> {
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT seg, data FROM cloud_segments WHERE session_id = ? AND sensor = ? AND seg BETWEEN ? AND ?")) {
                ps.setString(1, sessionId);
                ps.setInt(2, sensor);
                ps.setLong(3, lo);
                ps.setLong(4, hi);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        long seg = rs.getLong(1);
                        if (wanted.contains(seg)) {
                            out.put(seg, Segment.decode(seg, rs.getBytes(2)));
                        }
                    }
                }
            }
            return out;
        });
    }

    @Override
    public void saveSegments(String sessionId, int sensor, Collection<Segment> segments) {
        if (segments.isEmpty()) {
            return;
        }
        withConn(true, c -> {
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO cloud_segments(session_id, sensor, seg, present_count, data) VALUES(?, ?, ?, ?, ?)"
                            + " ON CONFLICT (session_id, sensor, seg) DO UPDATE SET present_count = excluded.present_count, data = excluded.data")) {
                for (Segment s : segments) {
                    ps.setString(1, sessionId);
                    ps.setInt(2, sensor);
                    ps.setLong(3, s.seg());
                    ps.setInt(4, s.count());
                    ps.setBytes(5, s.encode());
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            return null;
        });
    }

    @Override
    public void forEachSegment(String sessionId, int sensor, Consumer<SegmentSummary> consumer) {
        withConn(true, c -> {
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT seg, data FROM cloud_segments WHERE session_id = ? AND sensor = ? ORDER BY seg")) {
                ps.setFetchSize(200);
                ps.setString(1, sessionId);
                ps.setInt(2, sensor);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        consumer.accept(new SegmentSummary(rs.getLong(1), rs.getBytes(2)));
                    }
                }
            }
            return null;
        });
    }

    @Override
    public void insertLosses(String sessionId, List<LossRange> losses) {
        if (losses.isEmpty()) {
            return;
        }
        withConn(true, c -> {
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO cloud_losses(session_id, sensor, from_index, to_index, reason) VALUES(?, ?, ?, ?, ?)"
                            + " ON CONFLICT (session_id, sensor, from_index) DO NOTHING")) {
                for (LossRange l : losses) {
                    ps.setString(1, sessionId);
                    ps.setInt(2, l.sensor());
                    ps.setLong(3, l.fromIndex());
                    setLong(ps, 4, l.toIndex());
                    ps.setString(5, l.reason());
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            return null;
        });
    }

    @Override
    public List<LossRange> losses(String sessionId) {
        return withConn(false, c -> {
            List<LossRange> out = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT sensor, from_index, to_index, reason FROM cloud_losses WHERE session_id = ? ORDER BY sensor, from_index")) {
                ps.setString(1, sessionId);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        long to = rs.getLong(3);
                        Long t = rs.wasNull() ? null : to;
                        out.add(new LossRange(rs.getInt(1), rs.getLong(2), t, rs.getString(4)));
                    }
                }
            }
            return out;
        });
    }

    @Override
    public void upsertResults(String sessionId, List<StoredResult> results, String computedAt) {
        if (results.isEmpty()) {
            return;
        }
        withConn(true, c -> {
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO cloud_results(session_id, win, mode, body, computed_at) VALUES(?, ?, ?, ?, ?)"
                            + " ON CONFLICT (session_id, win, mode) DO UPDATE SET body = excluded.body, computed_at = excluded.computed_at")) {
                for (StoredResult r : results) {
                    ps.setString(1, sessionId);
                    ps.setLong(2, r.window());
                    ps.setString(3, r.mode());
                    ps.setString(4, r.body());
                    ps.setString(5, computedAt);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            return null;
        });
    }

    @Override
    public void forEachResult(String sessionId, String mode, Consumer<StoredResult> consumer) {
        withConn(true, c -> {
            String sql = "SELECT win, mode, body FROM cloud_results WHERE session_id = ?" + (mode == null ? "" : " AND mode = ?")
                    + " ORDER BY win, mode";
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setFetchSize(500);
                ps.setString(1, sessionId);
                if (mode != null) {
                    ps.setString(2, mode);
                }
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        consumer.accept(new StoredResult(rs.getLong(1), rs.getString(2), rs.getString(3)));
                    }
                }
            }
            return null;
        });
    }

    @Override
    public Set<Long> resultWindows(String sessionId, String mode) {
        return withConn(false, c -> {
            Set<Long> out = new HashSet<>();
            try (PreparedStatement ps = c.prepareStatement("SELECT win FROM cloud_results WHERE session_id = ? AND mode = ?")) {
                ps.setString(1, sessionId);
                ps.setString(2, mode);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(rs.getLong(1));
                    }
                }
            }
            return out;
        });
    }

    @Override
    public void deleteSession(String sessionId) {
        withConn(true, c -> {
            for (String t : new String[]{"cloud_segments", "cloud_losses", "cloud_results", "cloud_sessions"}) {
                try (PreparedStatement ps = c.prepareStatement("DELETE FROM " + t + " WHERE session_id = ?")) {
                    ps.setString(1, sessionId);
                    ps.executeUpdate();
                }
            }
            return null;
        });
    }
}
