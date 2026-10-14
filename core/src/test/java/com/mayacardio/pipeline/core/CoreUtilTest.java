package com.mayacardio.pipeline.core;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CoreUtilTest {

    @Test
    void rangeSetMergesAdjacentAndOverlapping() {
        RangeSet s = new RangeSet();
        s.add(10, 19);
        s.add(20, 29);
        s.add(40, 49);
        s.add(25, 41);
        assertEquals("[10-49]", s.toString());
        assertEquals(40, s.count());
        s.add(0, 5);
        assertEquals("[0-5, 10-49]", s.toString());
        s.add(6, 9);
        assertEquals("[0-49]", s.toString());
    }

    @Test
    void rangeSetRemoveSplits() {
        RangeSet s = new RangeSet();
        s.add(0, 99);
        s.remove(10, 19);
        s.remove(50, 200);
        assertEquals("[0-9, 20-49]", s.toString());
        assertTrue(s.containsAll(20, 49));
        assertFalse(s.containsAll(9, 20));
        assertTrue(s.contains(0));
        assertFalse(s.contains(15));
    }

    @Test
    void rangeSetMinusAndOpenEnded() {
        RangeSet held = RangeSet.of(List.of(new long[]{0, 999}, new long[]{2000, 2999}));
        RangeSet cloud = RangeSet.of(List.of(new long[]{0, 499}, new long[]{2500, 2600}));
        RangeSet missing = held.minus(cloud);
        assertEquals("[500-999, 2000-2499, 2601-2999]", missing.toString());
        RangeSet open = new RangeSet();
        open.add(100, Long.MAX_VALUE);
        open.add(50, 60);
        open.add(61, 99);
        assertEquals("[50-inf]", open.toString());
        assertTrue(open.containsAll(1_000_000, 2_000_000));
        assertEquals(Long.MAX_VALUE, open.count());
    }

    @Test
    void roundingMatchesPythonRound() {
        assertEquals(new BigDecimal("0.2"), Rounding.r1(0.25));
        assertEquals(new BigDecimal("0.8"), Rounding.r1(0.75));
        assertEquals(new BigDecimal("0.3"), Rounding.r1(0.35));
        assertEquals(new BigDecimal("0.5"), Rounding.r1(0.45));
        assertEquals(new BigDecimal("-1.2"), Rounding.r1(-1.25));
        assertEquals(new BigDecimal("1200.0"), Rounding.r1(1200));
        assertEquals(new BigDecimal("0.0"), Rounding.r1(-0.04));
        assertEquals("72.0", Rounding.r1(71.96).toPlainString());
        assertThrows(IllegalArgumentException.class, () -> Rounding.r1(Double.NaN));
    }

    @Test
    void batchCodecRoundTrip() throws Exception {
        int[][] rows1 = {{Signal.MIN_VALUE, 0, Signal.MAX_VALUE}, {Signal.MAX_VALUE, -1, Signal.MIN_VALUE}, {1, 2, 3}};
        int[][] rows2 = {{400000, 150000}, {400010, 149990}};
        BatchCodec.Batch b = new BatchCodec.Batch("5f0c2a9e", 1234L,
                List.of(new SampleRun(1, 120000, rows1), new SampleRun(2, 7, rows2)));
        byte[] enc = BatchCodec.encode(b);
        BatchCodec.Batch d = BatchCodec.decode(enc);
        assertEquals("5f0c2a9e", d.sessionId());
        assertEquals(1234L, d.capturedAtMillis());
        assertEquals(2, d.runs().size());
        assertEquals(120000, d.runs().get(0).firstIndex());
        for (int i = 0; i < rows1.length; i++) {
            assertArrayEquals(rows1[i], d.runs().get(0).rows()[i]);
        }
        for (int i = 0; i < rows2.length; i++) {
            assertArrayEquals(rows2[i], d.runs().get(1).rows()[i]);
        }
        assertEquals(5, d.sampleCount());
    }

    @Test
    void batchCodecRejectsGarbage() {
        assertThrows(Exception.class, () -> BatchCodec.decode(new byte[]{1, 2, 3}));
    }

    @Test
    void exportLinesMatchSpecFormat() {
        assertEquals("{\"sensor\": 1, \"index\": 120000, \"values\": [720, 1200, 1560]}",
                ExportFormat.sampleLine(1, 120000, new int[]{720, 1200, 1560}));
        assertEquals("{\"sensor\": 2, \"from_index\": 4000, \"to_index\": 4399, \"reason\": \"buffer_overflow\"}",
                ExportFormat.lossLine(new LossRange(2, 4000, 4399L, "buffer_overflow")));
        assertEquals("{\"sensor\": 2, \"from_index\": 4000, \"to_index\": null, \"reason\": \"x\"}",
                ExportFormat.lossLine(new LossRange(2, 4000, null, "x")));
    }

    @Test
    void resultJsonRoundTrip() {
        WindowResult r = WindowProcessor.compute(TestSignals.ramp(4000), 0);
        String line = ResultJson.line("abc", ResultJson.ONLINE, r, ResultJson.timestamps("computed_at", "t"));
        WindowResult back = ResultJson.parse(Json.read(line));
        assertEquals(r, back);
        assertTrue(line.contains("\"mode\": \"online\""));
        assertTrue(line.contains("\"heart_rate\": null"));
    }
}
