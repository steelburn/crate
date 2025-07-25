.. highlight:: psql

.. _ref-explain:

===========
``EXPLAIN``
===========

Explain or analyze the plan for a given statement.

Synopsis
========

::

    EXPLAIN [ ANALYZE | VERBOSE ] statement
    EXPLAIN [ ( option [, ...] ) ] statement

    where option is:

        ANALYZE [ boolean ]
        COSTS [ boolean ]
        VERBOSE [ boolean ]

Description
===========

The ``EXPLAIN`` command displays the execution plan that the planner generates
for the supplied statement. The plan is returned as a nested object containing
the plan tree.

.. Hidden: Analyze to display costs in the EXPLAIN VERBOSE output.

    cr> ANALYZE;
    ANALYZE OK, 1 row affected (... sec)

The ``VERBOSE`` option, available through ``EXPLAIN VERBOSE`` or
``EXPLAIN (VERBOSE TRUE)``, provides a breakdown of the steps performed by the
optimizer. An example output looks like this::

    cr> EXPLAIN VERBOSE
    ... SELECT employees.id
    ... FROM employees, departments
    ... WHERE employees.dept_id = departments.id AND departments.name = 'IT';
    +------------------------------------------------------+----------------------------------------------------------------------+
    | STEP                                                 | QUERY PLAN                                                           |
    +------------------------------------------------------+----------------------------------------------------------------------+
    | Initial logical plan                                 | Eval[id] (rows=3)                                                    |
    |                                                      |   └ Filter[((dept_id = id) AND (name = 'IT'))] (rows=3)              |
    |                                                      |     └ Join[CROSS] (rows=108)                                         |
    |                                                      |       ├ Collect[doc.employees | [id, dept_id] | true] (rows=18)      |
    |                                                      |       └ Collect[doc.departments | [id, name] | true] (rows=6)        |
    | optimizer_rewrite_filter_on_cross_join_to_inner_join | Eval[id] (rows=0)                                                    |
    |                                                      |   └ Filter[(name = 'IT')] (rows=0)                                   |
    |                                                      |     └ Join[INNER | (dept_id = id)] (rows=3)                          |
    |                                                      |       ├ Collect[doc.employees | [id, dept_id] | true] (rows=18)      |
    |                                                      |       └ Collect[doc.departments | [id, name] | true] (rows=6)        |
    | optimizer_move_filter_beneath_join                   | Eval[id] (rows=3)                                                    |
    |                                                      |   └ Join[INNER | (dept_id = id)] (rows=3)                            |
    |                                                      |     ├ Collect[doc.employees | [id, dept_id] | true] (rows=18)        |
    |                                                      |     └ Filter[(name = 'IT')] (rows=1)                                 |
    |                                                      |       └ Collect[doc.departments | [id, name] | true] (rows=6)        |
    | optimizer_rewrite_join_plan                          | Eval[id] (rows=3)                                                    |
    |                                                      |   └ HashJoin[INNER | (dept_id = id)] (rows=3)                        |
    |                                                      |     ├ Collect[doc.employees | [id, dept_id] | true] (rows=18)        |
    |                                                      |     └ Filter[(name = 'IT')] (rows=1)                                 |
    |                                                      |       └ Collect[doc.departments | [id, name] | true] (rows=6)        |
    | optimizer_merge_filter_and_collect                   | Eval[id] (rows=3)                                                    |
    |                                                      |   └ HashJoin[INNER | (dept_id = id)] (rows=3)                        |
    |                                                      |     ├ Collect[doc.employees | [id, dept_id] | true] (rows=18)        |
    |                                                      |     └ Collect[doc.departments | [id, name] | (name = 'IT')] (rows=1) |
    | Final logical plan                                   | Eval[id] (rows=3)                                                    |
    |                                                      |   └ HashJoin[INNER | (dept_id = id)] (rows=3)                        |
    |                                                      |     ├ Collect[doc.employees | [id, dept_id] | true] (rows=18)        |
    |                                                      |     └ Collect[doc.departments | [id] | (name = 'IT')] (rows=1)       |
    +------------------------------------------------------+----------------------------------------------------------------------+
    EXPLAIN 6 rows in set (... sec)

When issuing ``EXPLAIN ANALYZE`` or ``EXPLAIN (ANALYZE TRUE)`` the plan of the
statement is executed and timings of the different phases of the plan are returned.

The ``COSTS`` option is by default enabled and can be disabled by issuing
``EXPLAIN (COSTS FALSE)``. The output of the execution plan does then exclude
the costs for each logical plan.

.. NOTE::

   The content of the returned plan tree as well as the level of detail of the
   timings of the different phases should be considered experimental and are
   subject to change in future versions. Also not all plan nodes provide
   in-depth details.


The output of ``EXPLAIN ANALYZE`` also includes a break down of the query
execution if the statement being explained involves queries which are executed
using Lucene.

.. NOTE::

   When a query involves an empty partitioned table you will see no breakdown
   concerning that table until at least one partition is created by inserting
   a record.


The output includes verbose low level information per queried shard. Since SQL
query :ref:`expressions <gloss-expression>` do not always have a direct 1:1
mapping to Lucene queries, the output may be more complex but in most cases it
should still be possible to identify the most expensive parts of a query
expression.  Some familiarity with Lucene helps in interpreting the output.

A short excerpt of a query breakdown looks like this::

    {
      "BreakDown": {
        "advance": 0,
        "advance_count": 0,
        "build_scorer": 0,
        "build_scorer_count": 0,
        "compute_max_score": 0,
        "compute_max_score_count": 0,
        "create_weight": 0.004095,
        "create_weight_count": 1,
        "match": 0,
        "match_count": 0,
        "next_doc": 0,
        "next_doc_count": 0,
        "score": 0,
        "score_count": 0
      },
      "QueryDescription": "x:[1 TO 1]",
      "QueryName": "PointRangeQuery",
      "SchemaName": "doc",
      "ShardId": 0,
      "TableName": "employees",
      "Time": 0.004096
    }

The time values are in milliseconds. Fields suffixed with ``_count`` indicate
how often an operation was invoked.
If the query is executed on a partitioned table, each query breakdown will also
contain the related ``PartitionIdent`` entry.

.. list-table::
    :header-rows: 1
    :widths: auto
    :align: left

    * - Field
      - Description
    * - ``create_weight``
      - A ``Weight`` object is created for a query and acts as a temporary
        object containing state. This metric shows how long this process took.
    * - ``build_scorer``
      - A ``Scorer`` object is used to iterate over documents matching the
        query and generate scores for them. Note that this includes only the
        time to create the scorer, not that actual time spent on the iteration.
    * - ``score``
      - Shows the time it takes to score a particular document via its
        ``Scorer``.
    * - ``next_doc``
      - Shows the time it takes to determine which document is the next match.
    * - ``advance``
      - A lower level version of ``next_doc``. It also finds the next matching
        document but necessitates that the calling query perform additional
        tasks, such as identifying skips. Some queries, such as conjunctions
        (``must`` clauses in Boolean queries), cannot use ``next_doc``. For
        those queries, ``advance`` is timed.
    * - ``match``
      - Some queries use a two-phase execution, doing an ``approximation``
        first, and then a second more expensive phase. This metric measures
        the second phase.
    * - ``*_count``
      - Records the number of invocations of the particular method. For
        example, ``"next_doc_count": 2``, means the ``nextDoc()`` method was
        called on two different documents. This can be used to help judge how
        selective queries are, by comparing counts between different query
        components.

.. NOTE::

   Individual timings of the different phases and queries that are profiled do
   not sum up to the ``Total``. This is because there is usually additional
   initialization that is not measured. Also, certain phases do overlap during
   their execution.

Parameters
==========

:statement:
  The statement for which a plan or plan analysis should be returned.

  Currently only ``SELECT`` and ``COPY FROM`` statements are supported.
