# Build brief (verbatim from the task instructions, excluding the SPEC which is in ASSESSMENT_SPEC.md)

IMPORTANT: the real mock and check_ground_truth are NOT available yet. Build a clearly separate Maven module "mock-standin" that implements the protocol above faithfully, including ramp and signal modes, the 10-second buffer, loss reporting, and switchable fault injection (stall, corrupt crc, duplicate reply, one-sensor outage, session restart). Also write a "check-standin" tool that compares an export against the standin's deterministic ground truth. Label both as stand-ins in the README. The pipeline must code strictly to the protocol so the real mock can be swapped in with no changes. List "obtain real mock and check_ground_truth, re-run all tests against them" in MANUAL_TODO.md.

Architecture to follow (record any deviation in DECISIONS.md)
- Modules: core (shared window/heart-rate logic and models, used by both device and cloud so results are identical), device (the "pipeline" CLI and daemon), cloud (receiver, storage, processing, results API; Spring Boot is fine), mock-standin, and tools/scripts.
- Device storage: SQLite (xerial sqlite-jdbc) in WAL mode with synchronous=FULL. One transaction per read reply. Primary key (session_id, sensor, idx) so duplicates are impossible. A separate losses table and a sync-state table.
- Crash recovery: on startup, resume each sensor from last committed index + 1 for the current session by re-reading with from_index (the mock buffers 10 s, so a quick restart can recover data not yet committed). Anything older than the mock buffer is recorded as a loss with a reason. State the real maximum loss on power cut honestly in the design note, derived from this design.
- Never use wall-clock time for ordering or timeouts (clock changes are injected); use monotonic time. Timestamps are for display only.
- Disk full: do not crash; record the affected ranges as losses with reason "disk_full" when possible and recover when space returns.
- Corrupt replies: discard and re-read with from_index. Duplicate replies: ignore via the primary key. Mock stalls: timeouts with retry. Session change: close the old session, record the unread tail as a loss (to_index null if unknown), start the new one.
- Connectivity: a heartbeat to the cloud health endpoint every 2 s with short timeouts and a hysteresis of a few seconds, so mode switches land inside 10 s. The collector never blocks on the network.
- Online: stream batches (about 1 s each) to the cloud over HTTPS with acknowledgements; cloud stores idempotently (upsert on session, sensor, idx), computes windows, and stores results with mode=online and a received timestamp.
- Offline: the device computes windows with the same core library and stores results with mode=offline.
- Catch-up: range-based reconciliation. The device asks the cloud which ranges it holds and sends the missing ones in large gzip-compressed batches, oldest first, in parallel with live data. Size it so 2 hours of backlog (about 5.76 million samples) completes in well under 15 minutes. The cloud recomputes windows for newly filled ranges.
- Auto-start: provide a systemd unit and an install script so the pipeline restarts after reboot or power return.
- Keep CPU, memory and disk writes low. Report them honestly.

Work order (commit after each)
1. Project skeleton, spec docs, CLAUDE.md, DECISIONS.md, MANUAL_TODO.md.
2. core module: models, window processing, heart-rate logic, with unit tests including edge cases (missing samples, null rules, rounding, the 100-sample gap, w < 4).
3. mock-standin and check-standin.
4. Device collector with SQLite storage, loss ledger, crash recovery, and tests against every injected fault.
5. Cloud service (run locally with Docker or embedded storage first), plus an interface so the storage backend can change to whichever free provider I choose.
6. Online streaming, mode switching, catch-up, and the status, export and results commands.
7. Failure-test harness that automates: kill -9 mid-run, network cut and restore, 2-hour-equivalent backlog (use a faster simulated clock or smaller scale and also a full-scale run if feasible), disk full, clock change, mock restarts. Each test writes a results file under evidence/.
8. ARM64 verification: build and run the device side in an ARM64 container or QEMU limited to 4 CPUs and 2 GB (for example docker buildx with --platform linux/arm64, cpus 4, memory 2g). Write a script that records average CPU, peak memory, MB written to disk, and MB sent over the network for a 1-hour online run. Run a shorter version now and keep the 1-hour script ready.
9. Docs: README (fresh-board install and run), design note (max 2 pages, honest guarantees), AI_DISCLOSURE.md (state that Claude Code was used), test evidence summary, and a demo script for the 5-minute video.
10. Research the best free cloud option that avoids paid tiers and card-triggered surprises (use Claude in Chrome to read current free-tier terms). Prepare deployment files and step-by-step deployment instructions for the top choice, plus a fallback. Do not sign up for anything.
11. Final self-review against all 8 completion criteria and the scoring table. List every known gap honestly.
