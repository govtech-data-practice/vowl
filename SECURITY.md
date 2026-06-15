# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in `vowl`, please report it through [GitHub Security Advisories](https://github.com/govtech-data-practice/Vowl/security/advisories/new).

**Please do not report security vulnerabilities through public GitHub issues.**

When reporting, please include:

- A description of the vulnerability
- Steps to reproduce the issue
- The potential impact
- Any suggested fixes (if applicable)

## Security Measures

Automated security checks run in CI on every push and pull request. See [`.github/workflows/security.yml`](.github/workflows/security.yml) for details.

Contributors can run security checks locally via `make security-scan` and `make security-audit`.

## Disclaimer

Please note that while GovTech conducts SAST scans prior to publishing the software or updates for “vowl” and may provide security updates on a “best efforts basis” from time to time, “vowl” is licensed under the MIT license, including the disclaimer relating to the software. Without prejudice and in addition to the terms of the license for “vowl”:

(a) use of the software is entirely at your own risk, and you shall not rely on the SAST scans (or any scans), security updates, or the fact that GovTech had published the software; and

(b) GovTech disclaims all warranties and representations of any kind, whether express or implied, and all liability of whatever nature, arising out of or in connection with the scans and security updates, including without limitation any warranty or representation that the software or updates are free from errors, malicious code, or security issues.

GovTech may, at its sole and absolute discretion, discontinue the scans and/or security updates at any time without giving any notice.