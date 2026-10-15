# Cloud deployment (free tier only, no card)

Provider terms were read on 2026-10-14 from the providers' public docs and pricing pages; no account was created. Free-tier terms change, so re-read them before signing up.

## What the service needs

One container (Java 17, heap ≤ 320 MB, a single instance) behind HTTPS, and durable storage: about 11 MB of compressed segments per hour of data plus results. The service itself is stateless across restarts; everything lives in the database.

## Options checked

| Provider | Free compute | Card | Free durable storage | Verdict |
|---|---|---|---|---|
| Hugging Face Spaces | Docker Spaces now require a paid plan; outbound ports limited | — | no | not usable |
| Koyeb | no free compute tier listed (plans from $29/month) | — | 5 h/month Postgres | not usable |
| **Render** | free web service: 512 MB, 0.1 CPU, 750 instance-hours/month; **sleeps after 15 min without inbound requests**; waking took 22.9-24.1 s in our measurements (below); may restart at any time | **no** (without a payment method, overages suspend instead of billing) | ephemeral disk; free Postgres expires after 30 days | **compute: chosen** |
| **Neon** | free Postgres: 1 GB/project, 100 CU-hours/month (≈400 h at 0.25 CU), scales to zero after 5 min idle, 5 GB egress/month | **no** | yes | **database: chosen** |
| Northflank Sandbox | always on, 2 services + 1 database | its FAQ mentions card verification; not confirmed either way | yes | fallback if sign-up needs no card |
| Supabase | free Postgres 500 MB, pauses after a week idle | no | yes | database fallback |
| Oracle Cloud Always Free | always-on VMs | card verification at sign-up | yes | excluded by the no-card rule |

## Measured under Render-like limits

Cloud image from `docker/cloud/Dockerfile`, `docker run --cpus 0.1 --memory 512m`, PostgreSQL 16 in a container on the same Docker network (so no internet latency). Details: `evidence/cloud-0.1cpu-startup-and-catchup.json`, `evidence/backlog-cloud-0.1cpu.json`.

| | Spring Boot 4 (before) | JDK HTTP server (now) |
|---|---|---|
| Cold start, `docker run` → first `/health` 200 | 106-115 s (66 s with lazy init + C1-only JIT) | **14-16 s** |
| Memory, idle | 125 MB | 39 MB |
| 2-hour backlog (5.76 M samples, 30 MB on the wire): copy complete after network restore | — | **156 s** |
| … all 7,200 online results computed | — | **221 s** |
| Memory during catch-up | — | 135 MB |

Because Spring Boot alone took longer than the whole switch-back budget at 0.1 CPU, the cloud now uses the JDK's built-in HTTP server with the same endpoints; storage, ingest and processing code are unchanged.

## Measured on the deployed service

Render free tier + Neon, reached over the internet from a GitHub Actions native ARM64 runner (`evidence/arm64-ci-history/`, runs 1-3; details in `evidence/SUMMARY.md`):

| | Measured |
|---|---|
| Wake-up after the service had slept: first request to first `/health` 200 (includes our own start) | **22.9-24.1 s** (5 measurements: 22.94, 24.13, 22.95, 22.89, 22.94 s) |
| Network cut → offline / restore → online (cloud awake) | 3.5-3.6 s / 3.8-3.9 s |
| 2-hour backlog (5.76 M and 5.77 M samples, 29.2 MB on the wire, accelerated): copy complete after restore | 84 s and 85 s |
| Online results, 1-hour soak | p50 772-1130 ms; 10,711 of 10,712 windows within 5 s (one took 5,497 ms in run 2) |

Each batch adds database round trips to Neon over the internet, which is why catch-up took 84-85 s here against 156 s locally at 0.1 CPU and 11 s with an unthrottled local cloud; the device sends 40,000-index batches, two in parallel, and halves the batch size if a request times out.

## How mode switching behaves with a sleeping or cold cloud

- **Healthy, awake cloud:** offline 5.5 s after a cut and online 1.0 s after restore with a local cloud; 3.5-3.6 s and 3.8-3.9 s with the deployed cloud.
- **Cloud asleep (Render free tier, after 15 minutes without requests):** while the device is online its 2-second heartbeat keeps the service awake. If the device was offline for more than 15 minutes, Render puts the service to sleep, and when the network returns the device stays **offline until the cloud answers `/health`**: measured at 22.9-24.1 s from the first request to the first `/health` 200 (5 measurements, our own start included), so the switch back takes about 25 s. During that time nothing is lost (the device keeps saving and processing locally), but the "back online within 10 s" criterion is missed. The first catch-up requests after an outage longer than a minute use a 120 s timeout, so they do not fail while the service warms up.
- **Mitigation:** a free external uptime monitor (e.g. UptimeRobot) calling `https://<service>.onrender.com/health` every 5 minutes keeps the service awake during device outages. This needs an account (manual step).
- Neon suspends its compute after 5 idle minutes; the first query after that waits for it to resume (usually under a few seconds). The heartbeat does not touch the database, so Neon can sleep while the device is offline.

## Steps (Render + Neon)

1. **Neon:** sign up at neon.com (GitHub or email, no card), create a project in a region close to your Render region (e.g. AWS us-east-1 with Render Ohio/Virginia, Frankfurt with Render Frankfurt), copy the connection string `postgresql://USER:PASSWORD@HOST/neondb?sslmode=require`.
2. **Render:** sign up (no card), *New → Blueprint*, pick the GitHub repository `tanaysinghh/mayacardio-pipeline`. `render.yaml` creates one free Docker web service from `docker/cloud/Dockerfile` with `/health` as health check. Paste the Neon string into `CLOUD_JDBC_URL` when asked. Render generates `CLOUD_API_TOKEN`; copy it from the service's *Environment* tab. The `PIPELINE_INTERPRET_*` variables must match the device's `interpret.*` keys (defaults match).
   - Without a blueprint: *New → Web Service* → the repo → Docker, Dockerfile path `docker/cloud/Dockerfile`, instance type Free, environment `CLOUD_STORAGE=postgres`, `CLOUD_JDBC_URL=...`, `CLOUD_API_TOKEN=<long random string>`.
3. **Check:** `curl https://<service>.onrender.com/health` → `{"status": "ok", ...}`; `curl -H "Authorization: Bearer $TOKEN" https://<service>.onrender.com/api/v1/sessions` → `[]`.
4. **Board:** `sudo ./scripts/install.sh --cloud-url https://<service>.onrender.com --token $TOKEN` (or edit `/etc/pipeline/pipeline.conf`, then `sudo systemctl restart pipeline`).
5. **Uptime monitor** on `/health` every 5 minutes (see above).

Neon limits to keep in mind: 1 GB holds roughly 80-90 hours of data; 100 CU-hours cover about 400 active hours a month; `DELETE /api/v1/sessions/{id}` (bearer token, one session at a time) removes a session's samples, losses and results; PostgreSQL reuses the freed space after autovacuum, so Neon's storage figure may fall only later. The ARM64 workflow (`.github/workflows/arm64-evidence.yml`) deletes every session it created this way, reading `CLOUD_URL` and `CLOUD_API_TOKEN` from the repository's Actions secrets.

## Fallbacks

- **Northflank Sandbox + Neon**, only if sign-up needs no card: free service from the repo, Dockerfile `docker/cloud/Dockerfile`, port 8080, the same environment variables. Sandbox services do not sleep.
- **Any always-on machine you control:** `docker build -f docker/cloud/Dockerfile -t pipeline-cloud . && docker run -d -p 8080:8080 -e CLOUD_API_TOKEN=... -e CLOUD_SQLITE_PATH=/data/cloud.db -v pipeline-cloud:/data pipeline-cloud` (embedded SQLite), exposed over HTTPS with a free tunnel (e.g. Cloudflare Tunnel, needs a free account). No sleep limit, but it is not a third-party cloud.

## Storage backends

`CLOUD_STORAGE=sqlite|postgres`. For another PostgreSQL-compatible provider only `CLOUD_JDBC_URL` changes (`postgres://user:pass@host/db` URLs are accepted as given). Another kind of store would implement `cloud/src/main/java/com/mayacardio/pipeline/cloud/store/CloudStore.java`.
