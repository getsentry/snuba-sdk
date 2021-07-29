.. image:: https://sentry-brand.storage.googleapis.com/sentry-logo-black.png
    :width: 280
    :target: https://sentry.io

See here for the full `documentation <https://getsentry.github.io/snuba-sdk/>`_.
test
======
Status
======

.. image:: https://img.shields.io/pypi/v/snuba-sdk.svg
    :target: https://pypi.python.org/pypi/snuba-sdk

.. image:: https://github.com/getsentry/snuba-sdk/workflows/tests/badge.svg
    :target: https://github.com/getsentry/snuba-sdk/actions

.. image:: https://img.shields.io/discord/621778831602221064
    :target: https://discord.gg/cWnMQeA

=========
Examples
=========

Snuba SDK is a tool that allows SnQL queries (the language that Snuba uses) to be built programatically. A SnQL query is represented by a Query object, and has a number of attributes corresponding to different parts of the query.

Queries can be created directly:

.. code-block:: python

    query = Query(
        dataset="discover",
        match=Entity("events"),
        select=[
            Column("title"),
            Function("uniq", [Column("event_id")], "uniq_events"),
        ],
        groupby=[Column("title")],
        where=[
            Condition(Column("timestamp"), Op.GT, datetime.datetime(2021, 1, 1)),
            Condition(Column("project_id"), Op.IN, Function("tuple", [1, 2, 3])),
        ],
        limit=Limit(10),
        offset=Offset(0),
        granularity=Granularity(3600),
    )


Queries can also be built incrementally:

.. code-block:: python

    query = Query("discover", Entity("events"))
    .set_select(
        [Column("title"), Function("uniq", [Column("event_id")], "uniq_events")]
    )
    .set_groupby([Column("title")])
    .set_where(
        [
            Condition(Column("timestamp"), Op.GT, datetime.datetime.(2021, 1, 1)),
            Condition(Column("project_id"), Op.IN, Function("tuple", [1, 2, 3])),
        ]
    )
    .set_limit(10)
    .set_offset(0)
    .set_granularity(3600)


Once the query is built, it can be translated into a SnQL query that can be sent to Snuba or a human readable query.

.. code-block:: python

    # Outputs a formatted SnQL query
    print(query.print())

This outputs:

.. code-block:: sql

    MATCH (events)
    SELECT title, uniq(event_id) AS uniq_events
    BY title
    WHERE timestamp > toDateTime('2021-01-01T00:00:00.000000')
    AND project_id IN tuple(1, 2, 3)
    LIMIT 10
    OFFSET 0
    GRANULARITY 3600

If an expression in the query is invalid (e.g. ``Column(1)``) then an ``InvalidExpression`` exception will be thrown.
If there is a problem with a query, it will throw an ``InvalidQuery`` exception when ``.validate()`` or ``.translate()`` is called.

=========
TODO List
=========

- Join support
- Have the Entity object take a set of columns
- Syntactic sugar

===========================
Contributing to the SDK
===========================

Please refer to `CONTRIBUTING.rst <https://github.com/getsentry/snuba-sdk/blob/master/CONTRIBUTING.rst>`_.

=========
License
=========

Licensed under MIT, see `LICENSE <https://github.com/getsentry/snuba-sdk/blob/master/LICENSE>`_.
