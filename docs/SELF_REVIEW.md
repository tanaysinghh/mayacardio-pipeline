# Self-review against the completion criteria and scoring

Status words: **met (tested)** = demonstrated by automated tests in this repo, against the stand-in mock; **designed, not yet tested** = argued from the design but no test here exercises it; **open** = not done.

## Completion criteria

| # | Criterion | Status | Evidence / gap |
|---|---|---|---|
| 1 | Every received sample on disk exactly once, unchanged | met (tested) | `check-standin` in every scenario: 0 duplicates, 0 wrong values; collector unit tests check every index. Gap: not yet run against the official mock and `check_ground_truth`. |
| 2 | Every missing sample listed with a reason | met (tested) | 0 silent loss in all scenarios, including corrupt/duplicate/stall/outage/restart faults, disk full and kill -9. Gap: the unread tail of an interrupted session is listed only when the new session is seen (D39). |
| 3 | Survives crash, reboot, power cut; stated loss bound | crash: met (tested); power cut: designed, not yet tested on hardware | kill -9 x3 → 0 loss; simulated power cut (SIGKILL of daemon + mock) lost 0.08 s. Stated bound ≤ 1.0 s per sensor. A real power pull on a Pi (page cache lost, SD flush behaviour) has not been done (MANUAL_TODO 8). |
| 4 | Online results within 5 s | met (tested, local cloud) | p50 0.67 s, max 0.72 s, 0 windows over 5 s. Gap: a real free-tier cloud adds network latency and may sleep (Render) — see DEPLOY_CLOUD.md. |
| 5 | Mode switches within 10 s, no loss or duplication | met (tested, local cloud) | offline 5.6 s after cut, online 1.0-1.5 s after restore; device and cloud exports identical. Gap: with Render's free tier a cloud that slept during a >15 min outage takes ~1 min to wake unless an uptime pinger is configured (MANUAL_TODO 6). |
| 6 | Cloud copy complete within 15 min after outages up to 2 h | met (tested, local cloud) | 2 h backlog (5.76 M samples, 26.8 MB) complete 9.1 s after restore. Gap: on the free tier the cloud has 0.1 CPU and a real network; extrapolated to minutes, not measured. |
| 7 | Results follow the processing logic exactly | met against our own independent reference | all ~15,000 results across scenarios match a separately written naive implementation. Gap: interpretations (rounding mode, sensor-1-only null rule, result JSON shape) must be checked against the official reference (MANUAL_TODO 2). |
| 8 | ARM64, ≤ 4 cores, ≤ 2 GB, resources reported | partly | runs under emulated ARM64 limited to 4 CPUs / 2 GB; 15-minute figures for emulated ARM64 and native amd64. Gap: the 1-hour figure on a real Pi 4 is a manual step; emulated CPU numbers are not Pi numbers. |

## Scoring view (honest self-assessment)

- **Data safety on device (35):** strongest area. One transaction per reply, primary key, ledger invariant enforced in the same transaction, recovery from the mock buffer, disk-full buffering with declared losses. Risks: no real power-pull test yet; SD cards that ignore flush; tail declaration timing (D39).
- **Online mode and switching (25):** fast on a local cloud. Main risk is the free host (sleep, 0.1 CPU) rather than the pipeline.
- **Complete cloud copy (20):** range reconciliation is simple and verified at full 2-hour scale locally. Risk: free-tier CPU and the board's uplink speed.
- **Correct results (10):** exact peak semantics (global-scan equivalent), but tie rounding and some field choices are interpretations.
- **Design note, code quality, resources (10):** design note within about 2 pages; low CPU and memory; disk writes ~336 MB/h are the weakest resource figure.
- **Deductions:** no silent loss or unflagged wrong value was observed in any test run.

## Known gaps (complete list)

1. Official `biosignal-mock` and `check_ground_truth` not used; protocol edge cases guessed (D17, D23).
2. No real Raspberry Pi 4 run: no real power pull, no Pi resource figures, no Pi JVM start-time measurement (the 10 s buffer covers restarts up to ~8 s).
3. Cloud not deployed (needs accounts); all latency and catch-up figures come from a local cloud.
4. Render free tier sleeps after 15 min idle; needs an external pinger to keep the 10 s switch-back promise after long outages.
5. Disk writes ~336 MB/h (≈ 8 GB/day) at 4 commits/s.
6. Single cloud instance; no authentication beyond a shared bearer token; HTTPS depends on the host.
7. If the daemon dies while buffering in memory during a disk-full period, those samples are lost and later labelled `buffer_overflow` by the mock rather than `disk_full`.
8. The demo video is not recorded.
