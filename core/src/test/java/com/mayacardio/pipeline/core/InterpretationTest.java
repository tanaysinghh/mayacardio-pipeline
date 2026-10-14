package com.mayacardio.pipeline.core;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InterpretationTest {

    @AfterEach
    void clear() {
        for (String k : Interpretation.KEYS) {
            System.clearProperty("pipeline." + k);
        }
    }

    private static void set(String key, String value) {
        System.setProperty("pipeline." + key, value);
    }

    @Test
    void defaultsAreUnchanged() {
        assertEquals("half_even,sensor1,flat,report,report", Interpretation.describe());
        assertEquals(new BigDecimal("0.2"), Rounding.r1(0.25));
    }

    @Test
    void roundingHalfUp() {
        set(Interpretation.ROUNDING, "half_up");
        assertEquals(new BigDecimal("0.3"), Rounding.r1(0.25));
        assertEquals(new BigDecimal("-0.3"), Rounding.r1(-0.25));
        set(Interpretation.ROUNDING, "banker");
        assertThrows(IllegalArgumentException.class, () -> Rounding.r1(0.25));
    }

    @Test
    void heartRateRuleCanRequireBothSensors() {
        MemorySignalView v = TestSignals.spikes(8000, 400, 1_000_000);
        MemorySignalView g = new MemorySignalView();
        for (long i = 0; i < 8000; i++) {
            g.put(1, i, v.load(1, i, i).row(i));
            if (i != 3000) {
                g.put(2, i, v.load(2, i, i).row(i));
            }
        }
        g.setResolvedEnd(1, 8000);
        g.setResolvedEnd(2, 8000);
        assertNotNull(WindowProcessor.compute(g, 9).heartRate());
        set(Interpretation.HR_MISSING, "both");
        assertNull(WindowProcessor.compute(g, 9).heartRate());
        assertNotNull(WindowProcessor.compute(g, 12).heartRate());
    }

    @Test
    void nestedLayoutRoundTrips() {
        WindowResult r = WindowProcessor.compute(TestSignals.ramp(4000), 0);
        set(Interpretation.RESULTS_LAYOUT, "nested");
        String line = ResultJson.line("s", ResultJson.ONLINE, r, null);
        assertTrue(line.contains("\"mean\": {\"sensor_1\": ["));
        assertFalse(line.contains("mean_1"));
        assertEquals(r, ResultJson.parse(Json.read(line)));
    }

    @Test
    void suppressionRules() {
        WindowResult empty = new WindowResult(7, 400, 400, null, null, null, null, null, null, null, null);
        WindowResult partial = WindowProcessor.compute(TestSignals.ramp(4000), 2);
        assertFalse(Interpretation.suppress(empty, Long.MAX_VALUE));
        assertFalse(Interpretation.suppress(partial, 1000));
        set(Interpretation.EMPTY_WINDOWS, "skip");
        assertTrue(Interpretation.suppress(empty, Long.MAX_VALUE));
        set(Interpretation.PARTIAL_FINAL_WINDOW, "skip");
        assertTrue(Interpretation.suppress(partial, 1000));
        assertFalse(Interpretation.suppress(partial, 1200));
    }
}
