# Cloud deployment (free tier only, no card)

Provider terms were read on 2026-10-14 from the providers' public docs and pricing pages; no account was created. Free-tier terms change, so re-read them before signing up.

## What the service needs

One container (Java 17, heap ≤ 320 MB, a single instance) behind HTTPS, and durable storage: about 11 MB of compressed segments per hour of data plus results. The service itself is stateless across restarts; everything lives in the database.

## Options checked

| Provider | Free compute | Card | Free durable storage | Verdict |
|---|---|---|---|---|
| Hugging Face Spaces | Docker Spaces now require a paid plan; outbound ports limited | — | no | not usable |
| Koyeb | no free compute tier listed (plans from $29/month) | — | 5 h/month Postgres | not usable |
| **Render** | free web service: 512 MB, 0.1 CPU, 750 instance-hours/month; **sleeps after 15 min without inbound requests, ~1 min to wake**; may restart at any time | **no** (without a payment method, overages suspend instead of billing) | ephemeral disk; free Postgres expires after 30 days | **compute: chosen** |
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

Not measured: Render's real shared vCPU, Render's wake-up time, Neon's network latency and resume from scale-to-zero. With Neon each batch adds a few database round trips over the internet, so expect catch-up to be slower than above; the device sends 40,000-index batches, two in parallel, and halves the batch size if a request times out.

## How mode switching behaves with a sleeping or cold cloud

- **Healthy, awake cloud:** offline within ~6 s of a cut, online within ~2-4 s of restore (measured locally: 5.6 s and 1.0-1.5 s).
- **Cloud asleep (Render free tier, after 15 minutes without requests):** while the device is online its 2-second heartbeat keeps the service awake. If the device was offline for more than 15 minutes, Render puts the service to sleep, and when the network returns the device stays **offline until the cloud answers `/health`**: Render's wake-up (~1 minute, from Render's docs) plus our 14-16 s start. During that time nothing is lost (the device keeps saving and processing locally), but the "back online within 10 s" criterion is missed. The first catch-up requests after an outage longer than a minute use a 120 s timeout, so they do not fail while the service warms up.
- **Mitigation:** a free external uptime monitor (e.g. UptimeRobot) calling `https://<service>.onrender.com/health` every 5 minutes keeps the service awake during device outages. This needs an account (manual step).
- Neon suspends its compute after 5 idle minutes; the first query after that waits for it to resume (usually under a few seconds). The heartbeat does not touch the database, so Neon can sleep while the device is offline.

## Steps (Render + Neon)

1. **Neon:** sign up at neon.com (GitHub or email, no card), create a project in a region close to your Render region (e.g. AWS us-east-1 with Render Ohio/Virginia, Frankfurt with Render Frankfurt), copy the connection string `postgresql://USER:PASSWORD@HOST/neondb?sslmode=require`.
2. **Render:** sign up (no card), *New → Blueprint*, pick the GitHub repository `tanaysinghh/mayacardio-pipeline`. `render.yaml` creates one free Docker web service from `docker/cloud/Dockerfile` with `/health` as health check. Paste the Neon string into `CLOUD_JDBC_URL` when asked. Render generates `CLOUD_API_TOKEN`; copy it from the service's *Environment* tab. The `PIPELINE_INTERPRET_*` variables must match the device's `interpret.*` keys (defaults match).
   - Without a blueprint: *New → Web Service* → the repo → Docker, Dockerfile path `docker/cloud/Dockerfile`, instance type Free, environment `CLOUD_STORAGE=postgres`, `CLOUD_JDBC_URL=...`, `CLOUD_API_TOKEN=<long random string>`.
3. **Check:** `curl https://<service>.onrender.com/health` → `{"status": "ok", ...}`; `curl -H "Authorization: Bearer $TOKEN" https://<service>.onrender.com/api/v1/sessions` → `[]`.
4. **Board:** `sudo ./scripts/install.sh --cloud-url https://<service>.onrender.com --token $TOKEN` (or edit `/etc/pipeline/pipeline.conf`, then `sudo systemctl restart pipeline`).
5. **Uptime monitor** on `/health` every 5 minutes (see above).

Neon limits to keep in mind: 1 GB holds roughly 80-90 hours of data; 100 CU-hours cover about 400 active hours a month; `DELETE /api/v1/sessions/{id}` frees space.

## Fallbacks

- **Northflank Sandbox + Neon**, only if sign-up needs no card: free service from the repo, Dockerfile `docker/cloud/Dockerfile`, port 8080, the same environment variables. Sandbox services do not sleep.
- **Any always-on machine you control:** `docker build -f docker/cloud/Dockerfile -t pipeline-cloud . && docker run -d -p 8080:8080 -e CLOUD_API_TOKEN=... -e CLOUD_SQLITE_PATH=/data/cloud.db -v pipeline-cloud:/data pipeline-cloud` (embedded SQLite), exposed over HTTPS with a free tunnel (e.g. Cloudflare Tunnel, needs a free account). No sleep limit, but it is not a third-party cloud.

## Storage backends

`CLOUD_STORAGE=sqlite|postgres`. For another PostgreSQL-compatible provider only `CLOUD_JDBC_URL` changes (`postgres://user:pass@host/db` URLs are accepted as given). Another kind of store would implement `cloud/src/main/java/com/mayacardio/pipeline/cloud/store/CloudStore.java`.
