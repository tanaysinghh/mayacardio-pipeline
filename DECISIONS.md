# Decisions and interpretations

Each entry: the ambiguity, the choice made, and why. Deviations from the build brief are marked **Deviation**.

## Build and tooling

- **D1. Java version.** Source/target release 17 (runs on Raspberry Pi OS `openjdk-17-jre-headless`); developed and tested on JDK 21.
- **D2. Spring Boot 4.1.1** for the cloud module (current stable at time of writing). Jackson 2 is used directly in `core`; Spring's own Jackson 3 is not used by our controllers (they return pre-serialised JSON), so the two do not interact.
- **D3. Maven wrapper** (`mvnw`, Maven 3.9.16) so a fresh board only needs a JDK.

## Spec documents

- **D4.** `docs/ASSESSMENT_SPEC.md` holds the SPEC section verbatim. The rest of the brief (architecture, stand-in instructions, work order) is saved verbatim in `docs/BUILD_BRIEF.md`.
