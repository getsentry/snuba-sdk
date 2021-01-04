<p align="center">
    <a href="https://sentry.io" target="_blank" align="center">
        <img src="https://sentry-brand.storage.googleapis.com/sentry-logo-black.png" width="280">
    </a>
</p>

# snuba-sdk - SDK for generating SnQL queries for Snuba

[![PyPi page link -- version](https://img.shields.io/pypi/v/sentry-sdk.svg)](https://pypi.python.org/pypi/sentry-sdk)
[![Discord](https://img.shields.io/discord/621778831602221064)](https://discord.gg/cWnMQeA)

### Build Status
![Tests](https://github.com/sentry/snuba-sdk/workflows/tests/badge.svg)

# TODO List

- Update build/dev tooling to use github actions
- Add missing clauses: having, orderby, limitby, totals
- Subscriptable support (measurements\[fcp.first\])
- Curried function calls
- unary conditions
- Handle array/tuple literals by converting to `array(...)` and `tuple(...)` functions implicitly
- Complex boolean conditions (AND/OR)
- Join support
- Have the Entity object take a set of columns
- Syntactic sugar


# Contributing to the SDK

Please refer to [CONTRIBUTING.md](https://github.com/getsentry/snuba-sdk/blob/master/CONTRIBUTING.md).

# License

Licensed under the BSD license, see [`LICENSE`](https://github.com/getsentry/snuba-sdk/blob/master/LICENSE)
