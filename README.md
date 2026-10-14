# MayaCardio biosignal pipeline

A crash-safe pipeline for a Raspberry Pi 4 class board (ARM64, 4 cores, 2 GB) that reads two sensors from the `biosignal-mock` service, saves every sample to local disk, streams to a free cloud service that computes a result for every 1-second window, keeps working offline with the same processing code, and sends everything the cloud is missing when the network returns.

- Design note (2 pages, guarantees and limits): [docs/DESIGN_NOTE.md](docs/DESIGN_NOTE.md)
- Failure-test evidence: [evidence/SUMMARY.md](evidence/SUMMARY.md)
- Cloud deployment (free tier): [docs/CLOUD_DEPLOYMENT.md](docs/CLOUD_DEPLOYMENT.md)
- AI use: [AI_DISCLOSURE.md](AI_DISCLOSURE.md)

> **Stand-ins.** The official `biosignal-mock` and `check_ground_truth` were not available while this was built. `mock-standin` (jar `biosignal-mock-standin.jar`) and `check-standin` (in `pipeline-tools.jar`) are **our own stand-ins** written from the published protocol. All test results in `evidence/` were produced against the stand-ins. The pipeline only depends on the wire protocol, so the official mock can be used without code changes.

## Repository layout

| Module | What it is |
|---|---|
| `core` | Shared models, window statistics, heart-rate peak detection, rounding, binary batch codec. The device and the cloud call the same code. |
| `device` | The `pipeline` program: daemon (`pipeline run`) plus `status`, `export`, `results`, `sessions`. Plain Java 17, SQLite (xerial sqlite-jdbc). |
| `cloud` | Spring Boot service: ingest, idempotent storage (SQLite or PostgreSQL), window processing, results and export API. |
| `mock-standin` | Stand-in for `biosignal-mock` (signal and ramp modes, 10 s buffer, loss reporting, fault injection over a control port). |
| `tools` | `check-standin` (ground-truth check), `compare` (device vs cloud export), `check-results` (independent naive reference for results), `failtest` (failure-test harness). |
| `scripts/` | `install.sh`, `pipeline` wrapper, systemd units, `measure-resources.sh`, `arm64-verify.sh`. |
| `docker/` | ARM64 test image (`docker/arm64`) and the cloud image (`docker/cloud`). |

## Install and run on a fresh board

Tested path: Raspberry Pi OS (64-bit, Bookworm) or any Debian/Ubuntu arm64/amd64. The board needs internet once to install Java (and to build if you have no prebuilt jars).

```sh
sudo apt-get update && sudo apt-get install -y git openjdk-17-jdk-headless
git clone <this repo> pipeline && cd pipeline
./mvnw -q -DskipTests package          # about 3-6 minutes on a Pi 4 the first time
mkdir -p dist && cp device/target/device-1.0.0-all.jar mock-standin/target/biosignal-mock-standin.jar \
   tools/target/pipeline-tools.jar cloud/target/cloud-1.0.0-all.jar dist/
sudo ./scripts/install.sh --cloud-url https://YOUR-CLOUD-HOST --token YOUR-TOKEN
```

`install.sh` creates the `pipeline` system user, installs to `/opt/pipeline`, writes `/etc/pipeline/pipeline.conf`, installs `pipeline.service` (starts at boot, `Restart=always`, `RestartSec=1`) and starts it. Data lives in `/var/lib/pipeline/pipeline.db`. Add `--with-mock-standin` to also install the stand-in mock as a service (testing only; with the official mock, start it the way its instructions say, on port 7100).

The pipeline does not wait for the network at boot: it starts collecting as soon as the filesystem is up and goes online when the cloud answers.

### Commands

```sh
pipeline status                                   # one JSON line: mode, samples_saved, samples_in_cloud, backlog, ...
pipeline export --session <ID> --from device      # JSON lines: samples (sensor 1 then 2, by index), then losses
pipeline export --session <ID> --from cloud
pipeline results --session <ID>                   # one line per window and mode (device offline + cloud online)
pipeline sessions                                 # sessions on the device with counts
journalctl -u pipeline -f                         # daemon log
```

`--session current` selects the latest session. Export line formats are exactly the ones in the spec. The wrapper runs commands as the `pipeline` user (via `sudo -n -u pipeline` for the invoking user configured by `install.sh`).

### Configuration (`/etc/pipeline/pipeline.conf`)

| Key | Default | Meaning |
|---|---|---|
| `mock.host`, `mock.port` | `127.0.0.1`, `7100` | the mock |
| `data.dir` | `/var/lib/pipeline` | SQLite database and disk reserve file |
| `run.dir` | `/run/pipeline` | status file (tmpfs, no disk writes) |
| `cloud.url`, `cloud.token` | empty | cloud base URL and bearer token; empty URL = offline only |
| `poll.interval.ms` | 500 | mock polling when caught up (sets the power-cut exposure, see design note) |
| `heartbeat.interval.ms` / `.timeout.ms` | 2000 / 1500 | connectivity check |
| `heartbeat.fail.threshold` / `.ok.threshold` | 2 / 2 | hysteresis for mode switches |
| `catchup.chunk.samples` / `catchup.parallel` | 40000 / 2 | catch-up batch size and concurrency |

Every key can be overridden by an environment variable `PIPELINE_<KEY>` with dots replaced by underscores (for example `PIPELINE_CLOUD_URL`).

## Cloud service

Run locally (embedded SQLite):

```sh
CLOUD_API_TOKEN=secret java -jar dist/cloud-1.0.0-all.jar --server.port=8080
```

Run against PostgreSQL (the free hosted option): `CLOUD_STORAGE=postgres CLOUD_JDBC_URL=postgresql://user:pass@host/db?sslmode=require`. See [docs/CLOUD_DEPLOYMENT.md](docs/CLOUD_DEPLOYMENT.md) for the recommended free deployment (Render free web service + Neon free Postgres, no card) and the fallback.

API: `GET /health`, `POST /api/v1/ingest` (gzip binary batches), `GET /api/v1/sessions`, `GET /api/v1/sessions/{id}/ranges|results|export|status`, `POST /api/v1/sessions/{id}/losses|meta|results`.

## Tests

```sh
./mvnw test                                                     # unit and integration tests (all modules)
java -jar dist/pipeline-tools.jar failtest all --jars dist      # failure scenarios, writes evidence/*.json
```

Failure scenarios: `baseline`, `kill9`, `netcut`, `mockfaults`, `powercut` (simulated), `backlog` (2 hours of data generated at 20x while offline, then real time), `diskfull` (needs a small filesystem, run in the Linux container), `clock` (needs CAP_SYS_TIME, run in the Linux container). See [evidence/SUMMARY.md](evidence/SUMMARY.md).

## Resource measurement

On the board, after the pipeline has been online for a minute:

```sh
sudo /opt/pipeline/bin/measure-resources.sh --duration 3600 --out resources-pi4.json
```

It reports average CPU, peak memory, MB written to storage by the daemon and MB sent on the network interface. `scripts/arm64-verify.sh --seconds 3600` does the same inside an ARM64 container limited to 4 CPUs and 2 GB (QEMU emulation on an x86 host; CPU figures from emulation are not representative of a Pi 4, see `evidence/resources/`).

## Stand-in mock and checker

`scripts/biosignal-mock` (`biosignal-mock.cmd` on Windows) and `scripts/check_ground_truth` (`.cmd`) are **stand-ins** with the command lines the assessment describes; `install.sh` puts them on the PATH. Start the stand-in exactly like the official mock:

```sh
biosignal-mock --mode signal --seed 7 --duration 3600 --port 7100
```

It accepts only those four options. Stand-in-only settings come from the environment: `BIOSIGNAL_MOCK_GT_DIR` (where it writes `<session_id>.json` ground truth, default `./mock-ground-truth`), `BIOSIGNAL_MOCK_CONTROL_PORT` (default port+1; accepts `stall S`, `corrupt N`, `duplicate N`, `outage SENSOR S`, `speed X`, `restart`, `status`), `BIOSIGNAL_MOCK_SPEED`.

```sh
pipeline export --session <ID> --from device > export.jsonl
check_ground_truth export.jsonl mock-ground-truth/<ID>.json     # JSON report; exit 0 = pass, 1 = fail
printf 'outage 2 8
' | nc 127.0.0.1 7101                       # inject a fault into the stand-in
```
