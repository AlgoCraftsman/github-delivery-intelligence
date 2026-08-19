# Security policy

## Supported versions

This repository is a local portfolio and demonstration project, not a hosted
production service. Security fixes target the current `main` branch. No tagged release
is currently supported.

## Reporting a vulnerability

Use GitHub's private vulnerability-reporting flow when it is available on the public
repository. Do not put credentials, private keys, real webhook payloads, exploit
details, or other sensitive material in a public issue.

If private reporting is unavailable, contact the maintainer through the repository
owner's GitHub profile and request a private channel before sharing details. Include
the affected component, reproduction conditions, likely impact, and a proposed fix if
known.

The maintainer will acknowledge a report, evaluate whether it affects the local demo
or a reusable component, and coordinate disclosure after a fix or documented
mitigation is available. Response timing is not guaranteed for this portfolio project.

The implemented controls, trust boundaries, local limitations, and production
hardening guidance are documented in [docs/security.md](docs/security.md).
