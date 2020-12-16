<p align="center">
    <a href="https://sentry.io" target="_blank" align="center">
        <img src="https://sentry-brand.storage.googleapis.com/sentry-logo-black.png" width="280">
    </a>
</p>

# snuba-sdk - SDK for generating SnQL queries for Snuba

[![Build Status](https://travis-ci.com/getsentry/snuba-sdk.svg?branch=master)](https://travis-ci.com/getsentry/snuba-sdk)
[![PyPi page link -- version](https://img.shields.io/pypi/v/sentry-sdk.svg)](https://pypi.python.org/pypi/sentry-sdk)
[![Discord](https://img.shields.io/discord/621778831602221064)](https://discord.gg/cWnMQeA)

# TODO List

- Add missing clauses: having, orderby, limitby, totals
- Subscriptable support (measurements\[fcp.first\])
- Curried function calls
- Handle array/tuple literals by converting to `array(...)` and `tuple(...)` functions implicitly
- Join support
- Complex boolean conditions (AND/OR)
- Have the Entity object take a set of columns
- Syntactic sugar


# Contributing to the SDK

Please refer to [CONTRIBUTING.md](https://github.com/getsentry/snuba-sdk/blob/master/CONTRIBUTING.md).

# License

Licensed under the BSD license, see [`LICENSE`](https://github.com/getsentry/snuba-sdk/blob/master/LICENSE)
