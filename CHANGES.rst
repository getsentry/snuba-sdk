Changelog and versioning
==========================

2.0.13
-----

- Expose a `serialize_to_mql` function on the `Request` object
    - This function will serialize `MetricsQuery` objects to MQL strings


2.0.12
-----

- Extend MQL grammar to support OR filters and parentheses


2.0.11
-----

- Add MQLContext to capture addition query information that cannot be expressed in MQL string
    - Add visitors to support MQLContext serialization
- Support curried functions in the MQL grammar


2.0.10
-----

- Extend the MQL grammar to support unquoted tag values and the usage of spaces as delimiters
  in the filter string


2.0.9
-----

- Add a basic version of Formula SnQL translator
    - This translator is not meant to be permanent, it should be replaced with a MQL translator
- Add MetricsQuery to MQL string encoder


2.0.8
-----

- Support the `limit` and `offset` field in the MetricsQuery
    - This will set the appropriate LIMIT and OFFSET clauses in the resulting SQL query,
    allowing for pagination
- Align MQL filters grammar with Discover filters grammar.


2.0.7
-----
- Introduce MQL grammar and parser
- Add `Formula` class to support formula queries.
- Removed `filters` and `groupby` in MetricsQuery class


2.0.6
-----
- Make the `granularity` field optional in the Rollup, so that it can
  be automatically inferred by the API layer


2.0.5
-----
- Support the TOTALS clause in the Rollup of a MetricsQuery
- Allow 10s granularity in MetricsQuery


2.0.4
-----
- Fix a bug where the groupby on the MetricsQuery was not being serialized


2.0.3
-----

- Bug fixes and changes to support using the MetricsQuery in production
- Add `granularity` to the Rollup to delineate the interval (the results
bucketing) vs. the granularity of the underlying data
    - `granularity` is mandatory for now
    - This means adding back the restrictions of having interval or totals but not both
- Use the interval to rollup the time automatically
    - This also means supporting arbitrary intervals
    - Automatically order timeseries by the rolled up time
- Allow AliasedExpression in the groupby clause. This is to support resolving
    in Sentry. The result of the query should have the original tag key, not the
    resolved tag number. With AliasedExpression we can have Clickhouse do the mapping
    automatically.



2.0.2
-----

- Add validators and translators for the MetricsQuery and all its child objects
- Rename some components to be consistent between Metric and Metrics
- Move MetricsScope and Rollup to the timeseries module
    - Also stop treating them as Expressions, so they can be visited in a more nuanced way

2.0.1
-----

### Various fixes & improvements

- add additional setters to metric query child classes (#119) by @enochtangg
- update contributing to reflect new release procedure (#118) by @evanh

2.0.0
------

- Add a new syntax for querying timeseries metrics
    - Add a MetricsQuery class that can be attached to a Request
    - Add a Timeseries class that is used to capture a simple timeseries query
    - Add various other classes to support the new syntax

1.0.5
------

- `tenant_ids` required field added previously is now optional to support gradual adoption

1.0.4
------

- Add `tenant_ids` required field to Request object so additional information about the request can be passed from Sentry.
    - "tenants" include referrer, organization ID, etc.

1.0.3
------

- Add the search issues time column to the legacy parser

1.0.2
------

- Add the parent_api back to the Request so it can be passed from Sentry.

1.0.1
------

- Modify column_name_re to allow for @ and / characters in column names.

1.0.0
------

- Add a Request class that is the main entry point for Snuba.
    - The Request class contains the dataset being queried, the Query being sent, and any flags on the request
    - Flags have been removed from the Query entirely and are now set on the Request
- Add an "app_id" flag to send the AppID to Snuba
- Renamed `snuba` function to `serialize`


0.1.5
------

- Modify ALIAS_RE to allow for @ char


0.1.4
------

- Fix for a bug in 0.1.2 where parent_api was incorrectly validated

0.1.3
------
- Modify ALIAS_RE to allow round brackets
- Surround alias in `AliasedExpression` and `CurriedFunction` with backticks on query visitor
when translating a Query Object to a SNQL Query


0.1.2
------
- Add support for ``team`` and ``feature`` tags for attribution in Snuba

0.1.1
------
- Add support for lambdas and identifiers, which in turn enable higher order functions like `arrayMap`.

0.1.0
------

- Move to Python 3.8 and drop support for Python 3.6. Sentry is now using 3.8 so this library can upgrade as well.
    - Use __future__.annotations where necessary
- Update all dependencies to latest and fix subsequent linting errors
    - Correctly chain exceptions
    - Follow PEP naming conventions for Exceptions: https://www.python.org/dev/peps/pep-0008/#exception-names
- Add Data Model concept to Entities for extra validation
- Create a type alias for sequences of conditions
- Use sequences for LIMIT BY and ARRAY JOIN since Snuba now supports those operations over multiple columns

0.0.26
------

- New release to fix dataclasses import issue with 3.8

0.0.25
------

- Add a parent_api flag that is used to track the name of the calling API

0.0.24
------

- Fix a bug in legacy converter that correctly handles infix conditions inside other functions

0.0.23
------

- Add an AliasedExpression class that is used if the expression is in the select or groupby, which allows an alias of the results returned from Snuba. The alias is not used in any other clauses and is not available in the generated query in Snuba.
- Fix ALIAS_RE to allow single letter aliases
- Allow datetimes in legacy function strings (add : and - to allowed character list)

0.0.22
------

- Allow square brackets in aliases

0.0.21
------

- Try to convert wrapped conditions on tags to always use string comparisons.

0.0.20
------

- Remove brittle, inconsistent and incomplete group by checks.

0.0.19
------

- Fix escaping in queries. Move escaping from the legacy parser to the translator.

0.0.18
------

- Add some more allowed characters to the function regex

0.0.17
------

- Allow importing directly from snuba_sdk, e.g. `from snuba_sdk import Column, Function`
- Fix bug where conditions on releases were being incorrectly parsed.

0.0.16
------

- Fix bug with weirdly escaped slashes


0.0.15
------

Features:
    - Support embedding expressions in lists/tuples
    - Add a "legacy" flag that gets sent to Snuba for tracking

Fixes:
    - Fix for "+" in numbers
    - Don't strip more than the outer quotes on a string
    - Strip backticks out of strings from legacy queries


0.0.14
------

- Add isort

0.0.13
---------
- Remove "transform"  as an aggregate function
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
