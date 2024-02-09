.. image:: https://sentry-brand.storage.googleapis.com/sentry-wordmark-dark-280x84.png
    :width: 280
    :target: https://sentry.io/?utm_source=github&utm_medium=logo

See here for the full `documentation <https://getsentry.github.io/snuba-sdk/>`_.

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

Snuba SDK is a tool that allows requests to Snuba to be built programatically. A Request consists of a Query, the dataset the Query is targeting, the AppID of the Request, and any flags for the Request. A Query object is a code representation of a SnQL or MQL query, and has a number of attributes corresponding to different parts of the query.

Requests and Queries can be created directly:

.. code-block:: python

    request = Request(
        dataset = "discover",
        app_id = "myappid",
        tenant_ids = {"referrer": "my_referrer", "organization_id": 1234}
        query = Query(
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
        ),
        flags = Flags(debug=True)
    )


Queries can also be built incrementally:

.. code-block:: python

    query = (
        Query("discover", Entity("events"))
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
    )


Once the request is built, it can be translated into a Snuba request that can be sent to Snuba.

.. code-block:: python

    # Outputs a formatted Snuba request
    request.serialize()

It can also be printed in a more human readable format.

.. code-block:: python

    # Outputs a formatted Snuba request
    print(request.print())

This outputs:

.. code-block:: JSON

    {
        "dataset": "discover",
        "app_id": "myappid",
        "query": "MATCH (events) SELECT title, uniq(event_id) AS uniq_events BY title WHERE timestamp > toDateTime('2021-01-01T00:00:00.000000') AND project_id IN tuple(1, 2, 3) LIMIT 10 OFFSET 0 GRANULARITY 3600",
        "debug": true
    }

If an expression in the query is invalid (e.g. ``Column(1)``) then an ``InvalidExpressionError`` exception will be thrown.
If there is a problem with a query, it will throw an ``InvalidQueryError`` exception when ``.validate()`` or ``.translate()`` is called.
If there is a problem with the Request or the Flags, an ``InvalidRequestError`` or ``InvalidFlagError`` will be thrown respectively.

============
MQL Examples
============

MQL queries can be built in a similar way to SnQL queries. However they use a ``MetricsQuery`` object instead of a ``Query`` object. The ``query`` argument of a ``MetricsQuery`` is either a ``Timeseries`` or ``Formula``, which is a mathemtical formula of ``Timeseries``.

The other arguments to the ``MetricsQuery`` are meta data about how to run the query, e.g. start/end timestamps, the granularity, limits etc.

.. code-block:: python

    MetricsQuery(
        query=Formula(
            ArithmeticOperator.DIVIDE.value,
            [
                Timeseries(
                    metric=Metric(
                        public_name="transaction.duration",
                    ),
                    aggregate="sum",
                ),
                1000,
            ],
        ),
        start=NOW,
        end=NOW + timedelta(days=14),
        rollup=Rollup(interval=3600, totals=None, granularity=3600),
        scope=MetricsScope(
            org_ids=[1], project_ids=[11], use_case_id="transactions"
        ),
        limit=Limit(100),
        offset=Offset(5),
    )


===========================
Contributing to the SDK
===========================

Please refer to `CONTRIBUTING.rst <https://github.com/getsentry/snuba-sdk/blob/master/CONTRIBUTING.rst>`_.

=========
License
=========

Licensed under FSL-1.0-Apache-2.0, see `LICENSE.md <https://github.com/getsentry/snuba-sdk/blob/master/LICENSE.md>`_.
