# Cloud deployment (free tier only)

Research done on 2026-10-14 by reading the providers' current public docs and pricing pages. No account was created. Free-tier terms change; re-read the linked pages before signing up.

## What the cloud service needs

- An always-reachable HTTPS endpoint running one container (Java 17, ~250-350 MB heap, a single instance).
- Durable storage that survives restarts and redeploys: about 10-12 MB per hour of data (both sensors, compressed segments) plus results. A judged run of a few hours fits in well under 1 GB.
- No sleeping while the device is offline for up to 2 hours, or a cold start fast enough that the device's switch back to online still lands inside 10 seconds (see "Sleep risk" below).

## Options checked

| Provider | Free compute? | Card needed? | Persistent storage on free tier | Verdict |
|---|---|---|---|---|
| Hugging Face Spaces | Docker Spaces now **require a paid plan** (only static and limited ZeroGPU Gradio Spaces are free); outbound ports limited to 80/443/8080 | — | disk not persistent | not usable |
| Koyeb | pricing page shows no free compute tier (plans from $29/month); free Postgres limited to 5 h/month | — | — | not usable |
| **Render** | free web service: 512 MB RAM, 0.1 CPU, 750 instance-hours/month (enough for one service 24/7); sleeps after **15 min without inbound traffic**, ~1 min to wake; may restart at any time | **No** (docs: without a payment method, overages suspend services instead of billing) | filesystem ephemeral; free Render Postgres **expires after 30 days** | **compute: top choice** |
| **Neon** | free Postgres: 1 GB storage/project, 100 CU-hours/project/month (≈400 h at 0.25 CU), scales to zero after 5 min idle, 5 GB egress/project/month | **No** | durable | **database: top choice** |
| Northflank Sandbox | "always-on, no sleeping", 2 free services + 1 free database | pricing FAQ describes card verification when a card is entered; could not confirm whether the sandbox requires one | yes (addon) | fallback if no card is required at sign-up |
| Oracle Cloud Always Free | Arm VMs, always on | card verification at sign-up (general knowledge; the page could not be read in this session) | yes | not recommended under the "no card" rule |
| Supabase | free Postgres 500 MB; projects pause after a week of inactivity | No | durable | database fallback |

## Recommended: Render (free web service) + Neon (free Postgres)

Both are card-free, so nothing can be charged. Limits and how they affect the assessment:

- **0.1 CPU on Render.** Live streaming is light. Catch-up after a 2-hour outage needs CPU: see the measured CPU cost in "Catch-up on a 0.1 CPU instance" below.
- **Sleep risk.** Render sleeps after 15 minutes with no inbound request. While the device is online its 2-second heartbeat keeps the service awake. During a device outage longer than 15 minutes the service sleeps, and the first request after the network returns waits about a minute for it to wake. During that minute the device stays offline (it keeps saving and processing locally, nothing is lost), but the "back online within 10 s" criterion would be missed. **Mitigation (manual step):** a free external uptime monitor (for example UptimeRobot, 5-minute interval) calling `https://<service>.onrender.com/health` keeps it awake. This needs an account and is listed in MANUAL_TODO.
- **Neon scale-to-zero** after 5 minutes idle: the first database query after an idle period waits for the compute to resume (usually well under a few seconds). The heartbeat does not touch the database, so it does not keep Neon awake. 100 CU-hours per month cover about 400 hours of active time at 0.25 CU, enough for an evaluation period but not for months of 24/7 streaming.
- **Neon egress 5 GB/month.** The cloud reads segments back mainly after a restart (cache warm-up) and for exports; one hour of data is about 11 MB of stored segments.

### Catch-up on a 0.1 CPU instance

In the local 2-hour-backlog run the cloud process used about 10 CPU-seconds to ingest 5.76 million samples and compute 7,200 windows (sampled from the process's CPU time before and after the catch-up). At 0.1 CPU that is roughly 100-120 s of wall time, plus database round trips to Neon (72 batches of ~200 segments, two at a time). The estimate is well inside 15 minutes, but it is an extrapolation: shared vCPUs on free tiers can be slower than the development machine, JIT warm-up on a tiny CPU takes longer, and the network link from the board sets a floor (26.8 MB at 1 Mbit/s is about 3.5 minutes). Verify on the real deployment (MANUAL_TODO 8).

Live streaming needs about 4 KB/s of uploads plus one result per second; the cloud's steady-state CPU for that is small (a few percent of one core locally).

### Steps

1. **Neon** (manual): sign up at neon.com with GitHub or email (no card), create a project in the region closest to the Render region you will pick (for example AWS us-east-1 with Render Oregon or Ohio; Frankfurt with Render Frankfurt), copy the connection string `postgresql://USER:PASSWORD@HOST/neondb?sslmode=require`.
2. **Push this repository to GitHub** (manual; the repo has no remote yet).
3. **Render** (manual): sign up (no card), New → Blueprint, select the repository. `render.yaml` defines one free Docker web service built from `docker/cloud/Dockerfile` with `/health` as health check. When asked, paste the Neon connection string into `CLOUD_JDBC_URL`. `CLOUD_API_TOKEN` is generated by Render; copy it from the service's Environment tab.
   - Without blueprints: New → Web Service → the repo → Runtime Docker, Dockerfile path `docker/cloud/Dockerfile`, instance type Free, env vars `CLOUD_STORAGE=postgres`, `CLOUD_JDBC_URL=...`, `CLOUD_API_TOKEN=<a long random string>`.
4. Check: `curl https://<service>.onrender.com/health` returns `{"status": "ok", ...}`; `curl -H "Authorization: Bearer $TOKEN" https://<service>.onrender.com/api/v1/sessions` returns `[]`.
5. On the board: `sudo ./scripts/install.sh --cloud-url https://<service>.onrender.com --token $TOKEN` (or edit `/etc/pipeline/pipeline.conf` and `sudo systemctl restart pipeline`).
6. Optional but recommended: an uptime monitor on `/health` every 5 minutes (see Sleep risk).

## Fallback A: Northflank Sandbox + Neon

Use only if sign-up does not require a card. Create a free service from the GitHub repo with Dockerfile `docker/cloud/Dockerfile`, port 8080, the same three environment variables, and a public HTTPS port. Sandbox services do not sleep, which removes the wake-up risk.

## Fallback B: any always-on machine you own

Run `docker build -f docker/cloud/Dockerfile -t pipeline-cloud . && docker run -d -p 8080:8080 -e CLOUD_API_TOKEN=... -e CLOUD_SQLITE_PATH=/data/cloud.db -v pipeline-cloud:/data pipeline-cloud` (embedded SQLite, no database account needed) and expose it over HTTPS with a free tunnel (for example a Cloudflare Tunnel, which needs a free Cloudflare account). This is free and has no sleep limit, but it is not a third-party cloud and depends on that machine staying up.

## Switching storage backends

The cloud reads `CLOUD_STORAGE` (`sqlite` or `postgres`). For any other PostgreSQL-compatible free provider only `CLOUD_JDBC_URL` changes (a `postgres://user:pass@host/db` URL is accepted as given by most providers). A different kind of store would implement `com.mayacardio.pipeline.cloud.store.CloudStore`.
