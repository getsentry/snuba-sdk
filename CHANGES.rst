Changelog and versioning
==========================

0.0.13
---------
- Fix for legacy queries that have raw string functions with nested aggregates
- Stop conditions on tags[...] from being converted to dates if the rhs was a date string.
- Some legacy queries use tuples for some of the fields. Convert them to lists where appropriate.

0.0.12
---------

- Small fix for legacy queries

0.0.11
---------

- Add arrayjoin support
- Expand column regex to allow any tag

0.0.10
---------

- Handle sets in legacy queries

0.0.9
---------

- Fix a bug with aliases in legacy queries

0.0.8
---------

- Add support for dry run flag

0.0.7
---------

- Update the Legacy converter to produce Queries that will emulate the original query as closely as possible. This way we can track migration success by ensuring the SDK and legacy calls are both producing the same Clickhouse SQL.

0.0.6
---------

- Added documentation for the SDK, generated from Sphinx. The docs are located at `<https://getsentry.github.io/snuba-sdk/>`_.
- The SDK is now feature compatible with the legacy JSON Snuba API, so anything that can be done with the JSON can be done using this SDK.

0.0.5
----------

- Some small bug fixes uncovered after doing an integration test with Sentry and Snuba.

0.0.4
----------

- This package was originally developed for Python 3.8+, however Sentry (the main user) is still using Python 3.6.
- The tox tests being used in Github Actions were not executing correctly, and so didn't report that this package was incompatible with 3.6.
- The package was refactored to work with Python 3.6, and the tox tests were removed from Github Actions (but left in for easier local testing).

0.0.3
----------

- Add a query visitor for printing, validating and translating a Query object to a SnQL query
- Allow arrays and tuples of scalars in Expressions
- Add a function that translates from JSON snuba to SnQL SDK

0.0.2
----------

- It is now possible to create a functioning Query, with basic validation. Also CI and release tools have all been set up.

0.0.1
----------

- Created blank repo with basic bootstrapping

Versioning Policy
------------------------------

This project follows [semver](https://semver.org/), with three additions:

- Semver says that major version ``0`` can include breaking changes at any time. Still, it is common practice to assume that only ``0.x`` releases (minor versions) can contain breaking changes while ``0.x.y`` releases (patch versions) are used for backwards-compatible changes (bugfixes and features). This project also follows that practice.

- All undocumented APIs are considered internal. They are not part of this contract.

- Certain features may be explicitly called out as "experimental" or "unstable" in the documentation. They come with their own versioning policy described in the documentation.

We recommend to pin your version requirements against ``0.x.*`` or ``0.x.y``.
Either one of the following is fine:

.. code-block:: python

    snuba-sdk>=0.10.0,<0.11.0
    snuba-sdk==0.10.1


A major release ``N`` implies the previous release ``N-1`` will no longer receive updates. We generally do not backport bugfixes to older versions unless they are security relevant. However, feel free to ask for backports of specific commits on the bugtracker.
