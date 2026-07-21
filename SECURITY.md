# Security policy

## Reporting a vulnerability

Please report vulnerabilities **privately** via GitHub's security advisories: open the repository's **Security** tab and use **"Report a vulnerability"** (GitHub private vulnerability reporting). Do not open a public issue for anything you believe is a security problem.

Include what you can: affected version, a reproduction, and impact assessment. You will get an acknowledgement in the advisory thread, and a fix or a documented mitigation before any public disclosure.

Things especially worth reporting for this package:

- SQL reaching a destination without parameter binding (the destination-adapter boundary requires positional placeholders; values must never enter SQL text)
- Secrets leaking into logs, traces, run-ledger rows, or alert-sink payloads
- The import-safety sandbox (Phase-2 discovery) failing to contain what it claims to contain
- Plugin loading executing code at registry *scan* time (scans must be metadata-only)

## Supported versions

The project is pre-1.0. Only the **latest 0.x release** receives security fixes; older 0.x releases are not patched — upgrade to the newest release.

| Version | Supported |
|---|---|
| latest 0.x | yes |
| older 0.x | no |

See [VERSIONING.md](VERSIONING.md) for the stability and deprecation policy.
