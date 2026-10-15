# AI tools used

This submission was built with **Claude Code** (Anthropic's agentic coding tool, model Claude Opus 5.5), working in this repository under my direction across several sessions.

What the AI did:

- Wrote the code in all modules (`core`, `device`, `cloud`, `mock-standin`, `tools`), the tests, the scripts, the Dockerfiles and the first drafts of every document in this repository.
- Ran the builds, the unit and integration tests, the failure scenarios (inside disposable containers), the resource measurements (native amd64 and emulated ARM64 containers) and the cloud measurements under Render-like limits, and recorded the outputs in `evidence/`.
- Audited the client against the assessment's mock protocol, and replaced the cloud's Spring Boot layer with the JDK HTTP server after measuring Spring Boot's start time at 0.1 CPU.
- Wrote the GitHub Actions workflow that produces the native ARM64 evidence on GitHub's free ARM64 runner against the deployed cloud, the scripts it runs and the script that copies its results into `evidence/`, and dry-ran those scripts locally against a local cloud container. After the run, it copied the downloaded results into `evidence/`, traced the higher disk-write figure to a polling inefficiency, and fixed and re-measured it locally. After a second run, it kept both runs' results as history and investigated one slow online result, using the run's files and, with my permission, read-only views of the Render and Neon logs and metrics; it found no cause and changed no pipeline behaviour for it, but added diagnostic logging (upload timings, per-window latency CSV) for a third, soak-only run. It did not see or handle the cloud's API token.
- Read public documentation and pricing pages of hosting providers (Hugging Face Spaces, Render, Koyeb, Neon, Northflank) through a browser extension to compare free tiers. It did not create accounts, log in, or enter any personal or payment details.

What was left to me: creating the cloud accounts and deploying, adding the token as a repository secret, starting the ARM64 workflow and downloading its results, the demo video, and obtaining and running the official `biosignal-mock` and `check_ground_truth`. No Raspberry Pi 4 was available, so no test ran on a real board and no real power pull was done.

All test evidence was produced against **stand-in** versions of the mock and the ground-truth checker that the AI wrote from the assessment's protocol description, not against the official tools.
