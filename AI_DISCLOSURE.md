# AI tools used

This submission was built with **Claude Code** (Anthropic's agentic coding tool, model Claude Opus 5.5), working in this repository under my direction across two sessions.

What the AI did:

- Wrote the code in all modules (`core`, `device`, `cloud`, `mock-standin`, `tools`), the tests, the scripts, the Dockerfiles and the first drafts of every document in this repository.
- Ran the builds, the unit and integration tests, the failure scenarios (inside disposable containers), the resource measurements (native amd64 and emulated ARM64 containers) and the cloud measurements under Render-like limits, and recorded the outputs in `evidence/`.
- Audited the client against the assessment's mock protocol, and replaced the cloud's Spring Boot layer with the JDK HTTP server after measuring Spring Boot's start time at 0.1 CPU.
- Read public documentation and pricing pages of hosting providers (Hugging Face Spaces, Render, Koyeb, Neon, Northflank) through a browser extension to compare free tiers. It did not create accounts, log in, or enter any personal or payment details.

What was left to me: creating the cloud accounts and deploying, running the tests on a real Raspberry Pi 4 (including a real power pull), the 1-hour resource run on the board, the demo video, and obtaining and running the official `biosignal-mock` and `check_ground_truth`.

All test evidence was produced against **stand-in** versions of the mock and the ground-truth checker that the AI wrote from the assessment's protocol description, not against the official tools.
