# Testing

## Levels

1. **Unit and integration tests** (`./mvnw test`):
   - `core`: window statistics, missing samples, null rules, `w < 4`, the 100-sample refractory rule (including a suppressed candidate that must not restart the refractory period), the "half of the 800-sample maximum" rule in exact integer arithmetic, the 800-sample history and next-sample availability rules, rounding (Python `round` semantics), and a randomized cross-check of the local peak algorithm against a naive scan from index 0 (gaps and dense candidates included). Batch codec, range sets, export line formats.
   - `mock-standin`: ramp formula, determinism, one ECG peak per beat at the scheduled rates, CRC over the exact data text, buffer overflow and outage loss reports, session end, corrupt/duplicate/restart faults over TCP.
   - `device`: reply parser (raw-text CRC including unusual spacing, uppercase CRC, bad shapes, out-of-range values, error replies, `to_index: null`), and the collector against an in-process stand-in at 5x speed for every injected fault: clean run, corrupt replies, duplicate replies, short stall, stall beyond the buffer, sensor outage, session restart, quick restart (recovery from the mock buffer), long downtime, disk full within the memory buffer, disk full beyond it. Each test checks that every index below `next_index` is stored exactly once with the generator's value or inside exactly one declared loss.
   - `cloud`: segment codec, Postgres URL conversion, and an end-to-end ingest test where batches arrive shuffled and duplicated, the registry is reloaded mid-way, and losses and session end arrive later; every window must equal the core library's result on the same data. The same test runs against PostgreSQL when `PG_TEST_URL` is set.
   - `tools`: the checker detects silent loss, duplicates and wrong values.

2. **Failure scenarios** (`java -jar dist/pipeline-tools.jar failtest <name> --jars dist`): separate processes for the mock stand-in, the cloud jar (SQLite) and the device daemon, with the device talking to the cloud through an in-process TCP proxy that can black-hole traffic (a network cut that looks like a WiFi drop: no RST, requests just hang). Each run ends with: device export, cloud export, `check-standin` against the mock's ground truth, device-vs-cloud comparison, and `check-results` against an independent naive implementation. Results go to `evidence/<scenario>.json`.

| Scenario | What happens | Pass condition |
|---|---|---|
| `baseline` | 120 s online | no loss, identical cloud copy, results match, every online window within 5 s |
| `kill9` | daemon killed with SIGKILL 3 times, restarted 2 s later (like systemd) | zero samples lost (recovered from the mock's 10 s buffer) |
| `netcut` | network black-holed for 60 s | offline within 10 s, online within 10 s, cloud copy complete within 15 min, no loss, no duplicates, online windows outside the outage within 5 s |
| `mockfaults` | corrupt x10, duplicate x10, stall 3 s, sensor 2 outage 8 s, sensor 1 outage 5 s, stall 15 s, mock restart | every undelivered sample declared with the mock's reason; no silent loss, duplicates or wrong values |
| `powercut` | SIGKILL of daemon and mock together, restart after 3 s | old session's unread tail declared; loss at most 1 s |
| `backlog` | 2 hours of data generated at 20x speed while the network is cut, then real time and network back | cloud copy complete within 15 min |
| `diskfull` | data directory filesystem filled, held, then freed | daemon survives; nothing lost within the memory buffer; beyond it, losses declared as `disk_full`; recovers |
| `clock` | system clock jumped +1 h, -2 h, +1 h while online | no loss, no spurious mode switch |

`diskfull` and `clock` need Linux (a small tmpfs and `CAP_SYS_TIME`), so they run in the Linux container. `diskfull` refuses to run unless `--small-fs` is a filesystem of at most 1 GB, and never writes more than 1 GB. `clock` changes the clock of the whole Docker VM (containers share the kernel clock); it jumps +1 h, -2 h, +1 h so the net change is zero. On Windows Git Bash, set `MSYS_NO_PATHCONV=1` first, otherwise container paths such as `/small` are rewritten into Windows paths:

```sh
export MSYS_NO_PATHCONV=1
docker build -f docker/arm64/Dockerfile -t pipeline-device:amd64 .
docker run --rm --tmpfs /small:size=64m -v "$PWD/evidence:/out" pipeline-device:amd64 failtest diskfull --small-fs /small --hold 20 --evidence /out --work /tmp/work
docker run --rm --cap-add SYS_TIME -v "$PWD/evidence:/out" pipeline-device:amd64 failtest clock --allow-clock-change --evidence /out --work /tmp/work
```

## What the scenarios do not prove

- The mock and the checker are stand-ins. Re-run everything against the official `biosignal-mock` and `check_ground_truth` (MANUAL_TODO).
- `powercut` kills processes; it does not drop the operating system's page cache or cut power to the storage. The durability argument for real power loss rests on SQLite WAL with `synchronous=FULL` (every commit is fsynced) and on the SD card honouring flushes. A real power-pull test on the Pi is in MANUAL_TODO.
- The cloud in these runs is on the same machine, so network latency and bandwidth are not realistic. Bytes sent are recorded so catch-up time can be estimated for a given link speed.
- Timings on a Windows/x86 development machine and in QEMU-emulated ARM64 are not Pi 4 timings.

## Real-board checklist (for the Pi 4)

1. `sudo ./scripts/install.sh --cloud-url ... --token ... --with-mock-standin` (or the official mock).
2. `pipeline status` shows `"mode":"online"` within a few seconds.
3. Network cut: `sudo ip link set wlan0 down` (or pull the Ethernet cable) for 2 minutes, watch `pipeline status` switch to offline, bring it back, watch `backlog` drain.
4. Process kill: `sudo systemctl kill -s SIGKILL pipeline` and watch systemd restart it (`journalctl -u pipeline`).
5. Power cut: pull the power while online, restore it, wait for boot, check `pipeline sessions` (old session closed, tail declared).
6. Disk full: `sudo fallocate -l <free space> /var/lib/fill.bin`, wait, `sudo rm /var/lib/fill.bin`.
7. Clock change: `sudo date -s '+1 hour'`, later `sudo date -s '-1 hour'` (disable NTP first: `sudo timedatectl set-ntp false`).
8. Export and check: `pipeline export --session current --from device > dev.jsonl` then run the official `check_ground_truth`.
9. One hour online with `sudo /opt/pipeline/bin/measure-resources.sh --duration 3600`.
