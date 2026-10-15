# MayaCardio biosignal pipeline

A crash-safe pipeline for a Raspberry Pi 4 class board (ARM64, 4 cores, 2 GB). It reads two sensors from `biosignal-mock`, saves every sample to local disk (one fsynced SQLite transaction per mock reply), streams to a free cloud service that computes a result for every 1-second window, keeps working offline with the same processing code, and sends everything the cloud is missing when the network returns. Every sample that is not saved is listed with a reason.

> **Validated against stand-ins, not the official tools.** The official `biosignal-mock` and `check_ground_truth` were not available while this was built. `biosignal-mock` and `check_ground_truth` in `scripts/` are **stand-ins** written from the "Mock service" section of the assessment, and every result in `evidence/` was produced against them. The pipeline depends only on the wire protocol, so the official mock can be used with no code change; re-running the tests against the official tools is still to be done.

> **No Raspberry Pi 4 run.** A real Pi 4 run was not possible (no board was available). ARM64 evidence comes from the **GitHub Actions ubuntu-24.04-arm native ARM64 runner, limited to 4 CPUs / 2 GB; not a Raspberry Pi 4; power cuts simulated by SIGKILL** (see [ARM64 evidence on GitHub Actions](#arm64-evidence-on-github-actions)). The Pi steps below are written and checked in containers but have not been run on a board.

- Design note (architecture, guarantees, power-cut loss): [docs/DESIGN_NOTE.md](docs/DESIGN_NOTE.md) ([PDF](docs/DESIGN_NOTE.pdf))
- Failure-test and resource evidence: [evidence/SUMMARY.md](evidence/SUMMARY.md)
- Cloud deployment (free tier, no card): [docs/CLOUD_DEPLOYMENT.md](docs/CLOUD_DEPLOYMENT.md)
- AI use: [AI_DISCLOSURE.md](AI_DISCLOSURE.md)

## Repository layout

| Path | What it is |
|---|---|
| `core` | Shared window statistics, heart-rate peak detection, rounding, binary batch codec. Device and cloud call the same code. |
| `device` | The `pipeline` program (Java 17, SQLite via xerial sqlite-jdbc): daemon `pipeline run` plus `status`, `export`, `results`, `sessions`, `verify`. |
| `cloud` | Cloud service (Java 17, JDK HTTP server, PostgreSQL or SQLite): ingest, idempotent storage, window processing, results and export API. |
| `mock-standin` | **Stand-in** for `biosignal-mock`: signal and ramp modes, 10 s buffer, `lost` reporting, fault injection. |
| `tools` | `check-standin` (**stand-in** ground-truth checker), `compare`, `check-results` (independent naive reference), `failtest` (failure harness), `summarize`. |
| `scripts/` | `install.sh`, `pipeline` wrapper, `biosignal-mock` and `check_ground_truth` launchers (+ `.cmd`), systemd units, `measure-resources.sh`, `arm64-verify.sh`, `ci-arm64-evidence.sh` and `fetch-ci-evidence.sh` (ARM64 evidence on GitHub Actions), example config. |
| `.github/workflows/arm64-evidence.yml` | Manually triggered workflow that builds, tests and measures on a native ARM64 runner against the deployed cloud. |
| `docker/` | `cloud/Dockerfile` (the deployed service), `arm64/Dockerfile` (device test image; also builds for amd64). |
| `render.yaml` | Render blueprint for the cloud service. |
| `evidence/` | Compact results of every failure scenario and the resource runs. |

## Install and run on a fresh Raspberry Pi 4

1. **Flash** Raspberry Pi OS Lite (64-bit) with Raspberry Pi Imager; in its settings enable SSH, set a user and Wi-Fi (or use Ethernet). Boot and `ssh <user>@<pi>`.
2. **Java and tools**
   ```sh
   sudo apt-get update && sudo apt-get install -y git openjdk-17-jdk-headless iptables
   ```
3. **Get and build** (3-8 minutes on a Pi 4; or build elsewhere and copy the four jars into `dist/`)
   ```sh
   git clone https://github.com/tanaysinghh/mayacardio-pipeline.git && cd mayacardio-pipeline
   ./mvnw -q -DskipTests package
   mkdir -p dist && cp device/target/device-1.0.0-all.jar cloud/target/cloud-1.0.0-all.jar \
      mock-standin/target/biosignal-mock-standin.jar tools/target/pipeline-tools.jar dist/
   ```
4. **Deploy the cloud** once, from any computer: [docs/CLOUD_DEPLOYMENT.md](docs/CLOUD_DEPLOYMENT.md). You get a URL such as `https://<service>.onrender.com` and an API token.
5. **Install on the Pi**
   ```sh
   sudo ./scripts/install.sh --cloud-url https://<service>.onrender.com --token <CLOUD_API_TOKEN> --with-mock-standin
   ```
   This creates the `pipeline` user, installs to `/opt/pipeline`, writes `/etc/pipeline/pipeline.conf`, installs and starts `pipeline.service` (starts at boot, `Restart=always`, `RestartSec=1`, does not wait for the network) and puts `pipeline`, `biosignal-mock` and `check_ground_truth` on the PATH. `--with-mock-standin` also runs the stand-in as a service with exactly `biosignal-mock --mode signal --seed 7 --duration 3600 --port 7100`. With the **official** mock, omit that flag, install the official binary as `/usr/local/bin/biosignal-mock` (never overwritten by `install.sh`) and start it with the same command line.
6. **Check**
   ```sh
   pipeline status          # "mode":"online", samples_saved rising, backlog about one second of data
   pipeline sessions
   pipeline verify          # SQLite integrity + loss-ledger check, "ok": true
   journalctl -u pipeline -f
   ```

Data lives in `/var/lib/pipeline/pipeline.db`, status in `/run/pipeline/status.json` (tmpfs). The daemon heap is capped at 192 MB (`/opt/pipeline/bin/pipeline`).

## Commands

```sh
pipeline status                                   # one JSON line: mode, samples_saved, samples_in_cloud, backlog, counters
pipeline export --session <ID> --from device      # JSON lines: samples (sensor 1 then 2, by index), then losses
pipeline export --session <ID> --from cloud
pipeline results --session <ID>                   # one line per window and mode (device offline + cloud online)
pipeline sessions                                 # sessions with counts
pipeline verify                                   # exit 0 when the database and the loss ledger are consistent
```

`--session current` selects the latest session. Export lines are exactly the spec's two shapes (`{"sensor": 1, "index": 120000, "values": [...]}`, `{"sensor": 2, "from_index": 4000, "to_index": 4399, "reason": "..."}`).

## Configuration (`/etc/pipeline/pipeline.conf`)

| Key | Default | Meaning |
|---|---|---|
| `mock.host`, `mock.port` | `127.0.0.1`, `7100` | the mock |
| `data.dir`, `run.dir` | `/var/lib/pipeline`, `/run/pipeline` | database; status file |
| `cloud.url`, `cloud.token` | empty | cloud base URL and bearer token; empty URL = offline only |
| `poll.interval.ms` | 500 | mock polling when caught up (sets the power-cut exposure) |
| `heartbeat.interval.ms`, `.timeout.ms`, `.fail.threshold`, `.ok.threshold` | 2000, 1500, 2, 2 | online/offline decision |
| `catchup.chunk.samples`, `catchup.parallel` | 40000, 2 | catch-up batch size and concurrency |
| `interpret.rounding` | `half_even` | or `half_up` |
| `interpret.hr.missing` | `sensor1` | or `both`: which sensors' missing samples null the heart rate |
| `interpret.results.layout` | `flat` | or `nested` (`"mean": {"sensor_1": [...], "sensor_2": [...]}`) |
| `interpret.empty.windows` | `report` | or `skip` windows where neither sensor has any sample |
| `interpret.partial.final.window` | `report` | or `skip` the last window of a closed session when it is incomplete |

Each key can also be set as an environment variable `PIPELINE_<KEY>` (dots → underscores). The `interpret.*` keys must have the same values on the device and in the cloud (`PIPELINE_INTERPRET_*`, see `render.yaml`); the cloud shows its values on `GET /`.

## Cloud service

```sh
CLOUD_API_TOKEN=secret PORT=8080 java -jar dist/cloud-1.0.0-all.jar                         # embedded SQLite (./cloud-data)
CLOUD_STORAGE=postgres CLOUD_JDBC_URL='postgresql://user:pass@host/db?sslmode=require' CLOUD_API_TOKEN=secret java -jar dist/cloud-1.0.0-all.jar
docker build -f docker/cloud/Dockerfile -t pipeline-cloud .                                 # the image Render builds
```

API: `GET /health`, `GET /` (info), and with `Authorization: Bearer <token>`: `POST /api/v1/ingest` (gzip binary batches), `GET /api/v1/sessions`, `GET /api/v1/sessions/{id}/ranges|results|export|status`, `POST /api/v1/sessions/{id}/losses|meta|results`, `DELETE /api/v1/sessions/{id}` (removes one session's samples, losses and results; the ARM64 workflow uses it to delete its test sessions).

## Stand-in mock and checker

```sh
biosignal-mock --mode signal --seed 7 --duration 3600 --port 7100          # stand-in; same command line as the official mock
pipeline export --session <ID> --from device > export.jsonl
check_ground_truth export.jsonl mock-ground-truth/<ID>.json                 # stand-in; JSON report, exit 0 = pass
printf 'outage 2 8\n' | nc 127.0.0.1 7101                                  # inject a fault into the stand-in
```

The launcher accepts only those four options. Stand-in-only settings come from the environment: `BIOSIGNAL_MOCK_GT_DIR` (ground-truth files, default `./mock-ground-truth`), `BIOSIGNAL_MOCK_CONTROL_PORT` (default port+1; commands `stall S`, `corrupt N`, `duplicate N`, `outage SENSOR S`, `mute SENSOR S`, `speed X`, `restart`, `status`), `BIOSIGNAL_MOCK_SPEED`.

## Reproducing the tests

```sh
./mvnw test        # unit and integration tests; the PostgreSQL variants also run when PG_TEST_URL is set
                   # (a local server without TLS needs ...?sslmode=disable)
```

The failure scenarios start the mock, the cloud and the device as separate processes, inject faults and check the outcome against ground truth, the cloud copy and the reference. They kill processes and fill a disk, so run them **inside a disposable Linux container**; the device test image contains Java, the jars, the harness and libfaketime:

```sh
export MSYS_NO_PATHCONV=1                     # Git Bash on Windows only
docker build --platform linux/amd64 -f docker/arm64/Dockerfile -t pipeline-device:amd64 .
docker run --rm -v "$PWD/evidence:/out" pipeline-device:amd64 failtest baseline,kill9,netcut,mockfaults,powercut --evidence /out --work /tmp/w
docker run --rm -v "$PWD/evidence:/out" pipeline-device:amd64 failtest backlog --data-minutes 120 --fast-speed 20 --mock-buffer-seconds 200 --evidence /out --work /tmp/w
docker run --rm --tmpfs /small:size=64m -v "$PWD/evidence:/out" pipeline-device:amd64 failtest diskfull --small-fs /small --hold 20 --tag within-buffer --evidence /out --work /tmp/w
docker run --rm -v "$PWD/evidence:/out" pipeline-device:amd64 failtest clock --clock-method faketime --evidence /out --work /tmp/w
java -jar dist/pipeline-tools.jar summarize evidence
```

The clock scenario shifts the wall clock of the test's processes with libfaketime, never the kernel clock. `failtest` also accepts `--mock-cmd "<command with {port}>"` and `--checker-cmd "<command with {export}>"` to use the official mock and checker, and `--cloud-url URL` (token from `--cloud-token` or `CLOUD_API_TOKEN`) for an external cloud. With `--net-cmd "<command with {action}>"` the network is cut and restored by that command (`cut`/`restore`) instead of the in-process proxy, and the device talks to the external cloud directly. `soak --minutes N` is the online resource run (`--mock-cmd`, `--mock-port`, `--device-launcher` select the exact mock command and the production launcher).

## Resource measurement

```sh
sudo /opt/pipeline/bin/measure-resources.sh --duration 3600 --out resources-pi4.json        # on the board, while online
scripts/arm64-verify.sh --seconds 3600 --cloud-url http://host.docker.internal:18091 --token <t>   # ARM64 container, 4 CPUs, 2 GB
```

Reports average CPU, peak RSS, MB written to storage (`/proc/PID/io`) and MB sent on the network interface; elapsed time comes from `/proc/uptime`, so clock changes cannot distort it.

## ARM64 evidence on GitHub Actions

`.github/workflows/arm64-evidence.yml` runs on the free native ARM64 runner (`runs-on: ubuntu-24.04-arm`, public repository) when started by hand (Actions > arm64-evidence > Run workflow, or `gh workflow run arm64-evidence.yml -f minutes=60 -f scenarios=true`). It needs two repository secrets: `CLOUD_URL` (the deployed cloud) and `CLOUD_API_TOKEN`; the token is only read from secrets, passed to containers through the environment and masked in logs.

1. `./mvnw -B verify` natively on ARM64, with a PostgreSQL 16 service container for the PostgreSQL tests.
2. Builds the device image natively and runs `scripts/ci-arm64-evidence.sh`: each run is one container limited to `--cpus 4 --memory 2g --memory-swap 2g` holding the stand-in mock, the device daemon and the harness, talking to the live cloud over the internet.
   - **Resource run** (`soak`, default 60 minutes): mock started with exactly `biosignal-mock --mode signal --seed 7 --duration 3600 --port 7100`, daemon started by the production launcher; average CPU, peak RSS, MB written, MB sent, plus result latency over the whole run against the live cloud.
   - **Failure scenarios** (optional): kill -9, network cut and restore (`docker network disconnect` / `connect` of the container), every mock fault, power cut **simulated** by SIGKILL, the 2-hour backlog, disk full in a 64 MB tmpfs, clock jumps with libfaketime. Nothing needs privileges.
   - **Live-cloud figures**: result latency, mode-switch times, catch-up time after a 60 s outage and after 2 hours, and the cloud's wake-up time at the start and (default) after 16 idle minutes.
   - The runner's `uname -a`, `lscpu` and `/proc/cpuinfo` are recorded, and every test session is deleted from the cloud at the end (`DELETE /api/v1/sessions/{id}`), also when the run fails.
3. Uploads the compact results as the artifact `arm64-evidence` (log tails separately as `arm64-evidence-logs`, not for the repository).

Then, with the GitHub CLI logged in:

```sh
scripts/fetch-ci-evidence.sh            # latest completed run, or pass a run id
```

It copies the results into `evidence/arm64-ci/` and `evidence/resources/`, regenerates `evidence/SUMMARY.md` and prints the row for the design note's resource table. Without a `CLOUD_URL` the script starts a local cloud container instead, which is how it was tested on an amd64 machine.

**Results of run 38730796747** (commit 4a3b665, Neoverse-N2 runner, live Render + Neon cloud; every scenario PASS, 13 of 13 test sessions deleted afterwards; details in [evidence/SUMMARY.md](evidence/SUMMARY.md)):

- Resource run, 1 hour online: 1.07 % of one core, 120.5 MB peak RSS, 264.55 MB/h written, 16.05 MB sent over the hour.
- Online latency: p50 772 ms, p95 1490 ms, max 2116 ms (none over 5 s).
- Network cut: offline after 3.5 s; online 3.9 s after the network returns.
- 2-hour backlog: 5.76 M samples caught up 84 s after restore. The test is accelerated: data is generated at about 20x speed and the stand-in's buffer is raised to 200 s.
- Render wake-up: 22.9 s and 24.1 s. After an outage over 15 minutes, the switch back to online can take about 25 s unless an uptime pinger keeps the service awake.
- Power cuts are simulated by SIGKILL.

The disk-write figure includes a polling inefficiency that was fixed after the run (design note, "Why 265 vs 176 MB/h"). A local run of the same setup with the fix wrote 186 MB/h, against 286 MB/h before it.
