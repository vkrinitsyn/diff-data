# Security Policy

## Supported versions

Security fixes go into the latest release of diff_data. Upgrade to the latest release to
get them.

| Version | Supported |
|---|---|
| 0.3.x (latest) | yes |
| older | no |

## Reporting a vulnerability

Do not open a public issue for a security problem. Report it by email to
**support@dbinvent.com**, with "diff_data security" in the subject, and include:

- the diff_data version and how it was built or installed;
- what an attacker can do, and the steps or a proof of concept that show it;
- any fix or mitigation you know of.

We acknowledge the report, keep you informed while we fix it, and credit you in the
release notes unless you ask us not to. Please give us the time to release a fix
before you disclose the problem publicly.

## Dependencies

The dependencies diff_data is built with are pinned in `Cargo.lock`. CI checks them on
every push and pull request, and weekly: `cargo audit` against the RustSec advisory
database (`.github/workflows/audit.yml`), and Trivy over the repository, its findings in
Code scanning (`.github/workflows/trivy.yml`). Dependabot proposes their updates weekly.
