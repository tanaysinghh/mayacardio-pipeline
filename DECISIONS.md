# Decisions and interpretations

Each entry: the ambiguity, the choice made, and why. Deviations from the build brief are marked **Deviation**.

## Build and tooling

- **D1. Java version.** Source/target release 17 (runs on Raspberry Pi OS `openjdk-17-jre-headless`); developed and tested on JDK 21.
- **D2. Spring Boot 4.1.1** for the cloud module (current stable at time of writing). Jackson 2 is used directly in `core`; Spring's own Jackson 3 is not used by our controllers (they return pre-serialised JSON), so the two do not interact.
- **D3. Maven wrapper** (`mvnw`, Maven 3.9.16) so a fresh board only needs a JDK.

## Spec documents

- **D4.** `docs/ASSESSMENT_SPEC.md` holds the SPEC section verbatim. The rest of the brief (architecture, stand-in instructions, work order) is saved verbatim in `docs/BUILD_BRIEF.md`.

## Processing logic interpretations (core module)

- **D5. Peak definition is global and sequential.** "At least 100 samples after the previous peak" refers to the previous *peak* (not candidate), which makes peak status depend on history. The reference meaning is a scan from index 0 of the session. `PeakDetector` computes the same set locally: it searches backwards for an index `p` with no candidate in `[p-99, p-1]` (from there the greedy selection cannot depend on anything earlier) and runs the greedy scan from `p`, doubling the lookback when needed, down to the session start. A randomized unit test checks it against a naive global scan, including gaps and dense-candidate signals.
- **D6. "Larger than half of the maximum"** is evaluated exactly in integers as `2*v[i] > max(v[i-800..i-1])` (no floating point).
- **D7. "Available"** means the sample is stored (on the device) or held (in the cloud). Peak conditions require the 800 previous samples, the sample itself and the next sample to be available; a missing sample is never a peak.
- **D8. heart_rate null rule "any sample in those 5 seconds is missing"** is applied to sensor 1 only (heart rate is derived from sensor 1 channel 2). Missing sensor 2 samples do not null the heart rate.
- **D9. Heart-rate arithmetic** follows the spec literally: gaps are converted to seconds (`gap/400.0`), averaged, then `60/mean`. Different but mathematically equal orders can differ in the last floating-point bit; this only matters at exact rounding ties.
- **D10. Rounding** uses round-half-even on the exact binary value of the double (`new BigDecimal(x).setScale(1, HALF_EVEN)`), which is exactly Python's `round(x, 1)` and NumPy's `round`. Means of integer windows of 400 samples hit exact `.x5` ties (for example `.25`, `.75`) about once in 200 values, so the reference's rounding mode matters. If the reference turns out to use half-up, change `Rounding.MODE` in one place. Listed in MANUAL_TODO to verify against the real reference.
- **D11. Means** are `sum/count` with the integer sum computed exactly (long) then divided in double, which equals Python `sum(v)/len(v)`. min/max are integers (no rounding). `magnitude` sums `sqrt(c1^2+c2^2)` in index order.
- **D12. Result JSON shape** (not fixed by the spec): one line per window and mode with `session, window, mode, missing_1, missing_2, complete, mean_1, min_1, max_1, mean_2, min_2, max_2, magnitude, heart_rate` plus timestamps. Per-channel fields are arrays (sensor 1: 3 values, sensor 2: 2 values); the whole array is `null` when the sensor has no available sample in the window.
- **D13. Window readiness.** A window is computed only once every input index is *resolved*: either available or known to be lost (or beyond the end of a closed session). Sensor 1 needs `[400w, 400(w+1)]` (the extra sample is the "sample after" of the last candidate), sensor 2 needs `[400w, 400w+399]`, plus the heart-rate lookback when `w >= 4`. Because resolved inputs never change, a computed result is final; there is no "provisional" result.
- **D14. Batch wire format** between device and cloud is a small binary format (`BatchCodec`): gzip of runs of samples, delta + zig-zag varint per channel. It is several times smaller than JSON, which matters for catch-up time and the network figure.
