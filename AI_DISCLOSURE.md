# AI tools used

This submission was built with **Claude Code** (Anthropic's agentic coding tool, model Claude Opus 5.5) working in this repository under my direction.

What the AI did:

- Wrote the code in all modules (`core`, `device`, `cloud`, `mock-standin`, `tools`), the tests, the scripts, and the first drafts of every document in this repository.
- Ran the builds, unit tests, failure scenarios and the ARM64 container runs, and recorded the outputs in `evidence/`.
- Read public documentation pages of hosting providers (Hugging Face Spaces, Render, Koyeb, Neon, Northflank) through a browser extension to compare free tiers. It did not create accounts, log in, or enter any personal or payment details.

What was decided or must be done by a human is listed in `MANUAL_TODO.md` (cloud accounts and deployment, the real-board tests, the demo video, obtaining the official mock and checker).

Interpretations the AI made where the brief was ambiguous are recorded in `DECISIONS.md`. The test evidence was produced against **stand-in** versions of the mock and the ground-truth checker that the AI wrote from the published protocol, not against the official tools.
