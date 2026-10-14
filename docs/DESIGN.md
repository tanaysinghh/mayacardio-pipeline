# Design note

## Architecture

```
biosignal-mock ──TCP/NDJSON──► Collector ──1 txn per reply──► SQLite (WAL, synchronous=FULL)
  (on the board)               (1 thread)                       samples · losses · stream_state · sync_state · results
                                                                     │                         │
                         Heartbeat (GET /health, 2 s) ─► mode        ▼                         ▼
                                                        Live lane (last 10 s, 1/s)    Offline processor (core lib)
                                                        Catch-up lane (range diff,       results mode=offline
                                                        gzip binary, oldest first, x2)
                                                                     │ HTTPS + bearer token
                                                                     ▼
              Cloud (Spring Boot): ingest ─► segment store (PostgreSQL / SQLite) ─► window processor (core lib) ─► results mode=online
```

**Device (plain Java 17, one JVM).** The collector polls each sensor every 0.5 s (back-to-back when behind), always with `from_index = next expected index`. Each reply is validated (CRC-32 over the raw `data` text exactly as received, shape, 24-bit range) and committed in **one SQLite transaction**: new samples, loss records, the per-sensor `next_index` and the stored-range index. Corrupt replies are discarded and re-read; duplicated or late replies are self-describing (`first_index`) so they can never shift data, and the primary key `(session, sensor, idx)` rejects any re-insert. Stalls hit a 2 s monotonic timeout, the request is re-sent and after 3 timeouts the connection is reopened. A new `session_id` closes the old session with `[next_index, null]` declared lost. The collector never touches the network.

**Loss ledger invariant.** For every session and sensor, every index below `next_index` is either stored or inside exactly one loss record with a reason (the mock's `lost` reason where given: `buffer_overflow`, `sensor_outage`; otherwise ours: `not_delivered_by_mock`, `mock_session_restarted_unread_tail`, `disk_full`). Nothing is ever filled in.

**Cloud.** Batches are merged idempotently into 400-sample segments (presence bitmap + compressed values) so a duplicate never overwrites and an hour of data is ~11 MB. A window is computed only when all of its inputs are *resolved* (held, or declared lost, or past the end of a closed session), so every stored result is final. The device and the cloud call the same `core` code; heart-rate peaks are computed exactly as a scan from index 0 would compute them (a local anchor search, cross-checked against a global scan in tests).

**Switching and catch-up.** Two failed heartbeats → offline, two good ones → online; in-flight uploads are aborted on every switch. Offline, the device computes windows itself. Back online, the live lane restarts at "now − 10 s" (so live windows have their 5 s heart-rate context at once) while the catch-up lane asks the cloud which ranges it holds and sends the difference in 40,000-index gzip batches, two at a time.

## What is guaranteed, and under which assumptions

| Claim | Basis | Assumes |
|---|---|---|
| A sample acknowledged by a commit survives a crash, reboot or power cut | WAL with `synchronous=FULL` fsyncs every commit | the storage honours flush (SD cards that lie about flush can lose recent commits) |
| Every sample is on disk at most once | primary key; overlap skipped before insert | — |
| No silent loss: every missing index is listed with a reason | ledger invariant, enforced in the same transaction | the database is not corrupted |
| Restart of the daemon loses nothing if it is back within ~8 s | resume from `next_index` with `from_index`; the mock buffers 10 s | the mock kept running (systemd restarts us in ~1 s, JVM start ~1-3 s on a Pi) |
| Offline within 10 s, online within 10 s of the network changing | ≤ 2 s + 2 s + 1.5 s timeout to go offline; ≤ ~4 s to go online | the cloud host is awake (see limits) |
| Cloud copy complete after an outage | range reconciliation repeats until the difference is empty | the cloud is reachable and fast enough (measured below) |
| Results identical online and offline | same code, computed only on resolved inputs | rounding/HR interpretations in DECISIONS D5-D13 match the reference |

**Not guaranteed / known limits.** (1) Data the mock generated but we had not yet committed when power is lost (the mock dies with the board). (2) The unread tail of an interrupted session is listed only once the device sees the new session (D39). (3) If the disk stays full beyond the 2-minute memory buffer, the oldest buffered samples are dropped and declared `disk_full`; if the process dies while buffering, those samples are lost and later appear as the mock's `buffer_overflow`. (4) The 5 s online latency depends on the cloud host: on Render's free tier the service sleeps after 15 minutes without requests, so after a long outage the first minute back is spent waking it (data unaffected; an external pinger avoids it). (5) One cloud instance only. (6) Wall-clock time is used only for display; all timeouts and ordering use the monotonic clock.

## Maximum data loss on a power cut

Per sensor, data reaches disk only by commit, and commits happen every poll (0.5 s when caught up) plus the time to read and commit one reply (normally 5-30 ms). At the instant of a power cut, the samples generated since the last commit are either still in the mock's memory or in our process memory, and both are gone (the mock runs on the same board and restarts with a new session). So the loss is **one poll interval plus one read-and-commit, about 0.5-0.6 s per sensor**, measured maximum gap between commits **~0.53 s** in all test runs. We state **at most 1.0 s per sensor** to leave margin for scheduling or I/O stalls on the Pi. The lost range is recorded on the next boot as `[next_index, null]`, reason `mock_session_restarted_unread_tail`. The simulated power cut (SIGKILL of daemon and mock) lost 0.025-0.08 s. This bound assumes the SD card honours fsync and the disk was not full at the time.

## Resource use

| Environment | Avg CPU (of 4 cores) | Peak RSS | Disk written | Network sent |
|---|---|---|---|---|
| RESOURCE_ROWS |

Measured with `scripts/measure-resources.sh` (CPU from `/proc/PID/stat`, RSS, `write_bytes` from `/proc/PID/io`, interface bytes from `/proc/net/dev`). Writes are dominated by SQLite (row data, WAL and checkpoints; ~4 commits per second). The 1-hour figure on a real Pi 4 is a manual step (MANUAL_TODO).
