Schema Examples
------------------------

When defining an ``EntityModel``, you first pass in all the columns that are supported by that Entity, marking any columns that must be included in the conditions of a query with the ``required`` flag. You then specify which column is the time column for your entity. When building a query, if it references a column that is not defined in the ``EntityModel``, an exception will be thrown. Similarly a query is not valid if it does not have conditions for all the required columns and the required time column. Required columns must have a top level condition using the ``Op.IN`` or ``Op.EQ``, and the required time column must have conditions for both ``Op.GTE`` and the ``Op.LT``.

.. code-block:: python

    SCHEMA = EntityModel(
        columns=[
            ColumnModel(name="test1"),
            ColumnModel(name="test2"),
            ColumnModel(name="required1", required=True),
            ColumnModel(name="required2", required=True),
            ColumnModel(name="time"),
        ],
        required_time_column=ColumnModel(name="time"),
    )
    ENTITY = Entity(
        name="test",
        alias=None,
        sample=None,
        data_model=SCHEMA
    )

    # ENTITY can now be passed into the match clause of a query, and/or into a Column for Joins.
