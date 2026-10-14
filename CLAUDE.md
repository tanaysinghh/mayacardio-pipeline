# CLAUDE.md

Take-home assessment for MayaCardio's Software Systems Engineer role: a crash-safe biosignal pipeline for a Raspberry Pi 4 class board plus a free cloud service.

- The assessment text is in [docs/ASSESSMENT_SPEC.md](docs/ASSESSMENT_SPEC.md). It is the source of truth for requirements, protocol and completion criteria.
- The full build brief (architecture to follow, work order) is in [docs/BUILD_BRIEF.md](docs/BUILD_BRIEF.md).
- Ambiguities and deviations are recorded in [DECISIONS.md](DECISIONS.md). Steps that need a human are in [MANUAL_TODO.md](MANUAL_TODO.md).

## Working rules

- Java 17+ only. Plain Java on the device; Spring Boot allowed on the cloud side. Maven multi-module build (`./mvnw`).
- No comments in code.
- Prefer surgical edits over rewrites once files exist.
- Be honest in all docs about what is and is not guaranteed. No overclaiming.
- Free and open-source services only. Never enable anything paid.
- Browser use is for research only. Never create accounts, log in, or enter payment or personal details; those go in MANUAL_TODO.md.
- Work autonomously; when something is ambiguous, pick the most reasonable reading, record it in DECISIONS.md, and continue.
- Commit at each milestone with a clear message. Do not push (no remote yet).
- The pipeline must code strictly to the mock protocol so the real `biosignal-mock` can replace `mock-standin` with no changes.
- Never use wall-clock time for ordering or timeouts; use monotonic time. Wall-clock timestamps are display only.

## Modules

- `core`: shared models, window processing and heart-rate logic, batch codec. Used by device and cloud so results are identical.
- `device`: the `pipeline` CLI and daemon (collector, SQLite storage, uploader, offline processing).
- `cloud`: Spring Boot receiver, storage, processing and results API.
- `mock-standin`: stand-in for the real `biosignal-mock` (protocol plus fault injection).
- `tools`: `check-standin` ground-truth checker and the failure-test harness.
- `scripts/`: install, systemd, resource measurement, ARM64 container scripts.
