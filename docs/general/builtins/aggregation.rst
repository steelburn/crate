.. highlight:: psql
.. _aggregation:

===========
Aggregation
===========

When :ref:`selecting data <sql_dql_aggregation>` from CrateDB, you can use an
`aggregate function`_ to calculate a single summary value for one or more
columns.

For example::

   cr> SELECT count(*) FROM locations;
   +-------+
   | count |
   +-------+
   |    13 |
   +-------+
   SELECT 1 row in set (... sec)

Here, the :ref:`count(*) <aggregation-count-star>` function computes the result
across all rows.

Aggregate :ref:`functions <gloss-function>` can be used with the
:ref:`sql_dql_group_by` clause. When used like this, an aggregate function
returns a single summary value for each grouped collection of column values.

For example::

   cr> SELECT kind, count(*) FROM locations GROUP BY kind;
   +-------------+-------+
   | kind        | count |
   +-------------+-------+
   | Galaxy      |     4 |
   | Star System |     4 |
   | Planet      |     5 |
   +-------------+-------+
   SELECT 3 rows in set (... sec)


.. TIP::

    Aggregation works across all the rows that match a query or on all matching
    rows in every distinct group of a ``GROUP BY`` statement. Aggregating
    ``SELECT`` statements without ``GROUP BY`` will always return one row.


.. _aggregation-expressions:

Aggregate expressions
=====================

An *aggregate expression* represents the application of an :ref:`aggregate
function <aggregation-functions>` across rows selected by a query. Besides the
function signature, :ref:`expressions <gloss-expression>` might contain
supplementary clauses and keywords.

The synopsis of an aggregate expression is one of the following::

   aggregate_function ( * ) [ FILTER ( WHERE condition ) ]
   aggregate_function ( [ DISTINCT ] expression [ , ... ] ) [ FILTER ( WHERE condition ) ]

Here, ``aggregate_function`` is a name of an aggregate function and
``expression`` is a column reference, :ref:`scalar function <scalar-functions>`
or literal.

If ``FILTER`` is specified, then only the rows that met the
:ref:`sql_dql_where_clause` condition are supplied to the aggregate function.

The optional ``DISTINCT`` keyword is only supported by aggregate functions
that explicitly mention its support. Please refer to existing
:ref:`limitations <aggregation-limitations>` for further information.

The aggregate expression form that uses a ``wildcard`` instead of an
``expression`` as a function argument is supported only by the ``count(*)``
aggregate function.


.. _aggregation-functions:

Aggregate functions
===================


.. _aggregation-arbitrary:

``arbitrary(column)``
---------------------

The ``arbitrary`` aggregate function returns a single value of a column.
Which value it returns is not defined.

Its return type is the type of its parameter column and can be ``NULL`` if the
column contains ``NULL`` values.

Example::

    cr> select arbitrary(position) from locations;
    +-----------+
    | arbitrary |
    +-----------+
    |       ... |
    +-----------+
    SELECT 1 row in set (... sec)

::

    cr> select arbitrary(name), kind from locations
    ... where name != ''
    ... group by kind order by kind desc;
    +-...-------+-------------+
    | arbitrary | kind        |
    +-...-------+-------------+
    | ...       | Star System |
    | ...       | Planet      |
    | ...       | Galaxy      |
    +-...-------+-------------+
    SELECT 3 rows in set (... sec)

An example use case is to group a table with many rows per user by ``user_id``
and get the ``username`` for every group, that means every user. This works as
rows with same ``user_id`` have the same ``username``.  This method performs
better than grouping on ``username`` as grouping on number types is generally
faster than on strings.  The advantage is that the ``arbitrary`` function does
very little to no computation as for example ``max`` aggregate function would
do.


.. _aggregation-any-value:


``any_value(column)``
---------------------

``any_value`` is an alias for :ref:`arbitrary <aggregation-arbitrary>`.

Example::

    cr> select any_value(x) from unnest([1, 1]) t (x);
    +-----------+
    | any_value |
    +-----------+
    |         1 |
    +-----------+
    SELECT 1 row in set (... sec)


.. _aggregation-array-agg:

``array_agg(column)``
---------------------

The ``array_agg`` aggregate function concatenates all input values into an
array.

::

    cr> SELECT array_agg(x) FROM (VALUES (42), (832), (null), (17)) as t (x);
    +---------------------+
    | array_agg           |
    +---------------------+
    | [42, 832, null, 17] |
    +---------------------+
    SELECT 1 row in set (... sec)

.. SEEALSO::

    :ref:`aggregation-string-agg`


.. _aggregation-avg:

``avg(column)``
---------------

The ``avg`` and ``mean`` aggregate function returns the arithmetic mean, the
*average*, of all values in a column that are not ``NULL``. It accepts all
numeric, timestamp and interval types as single argument. For ``numeric``
argument type the return type is ``numeric``, for ``interval`` argument type the
return type is ``interval`` and for other argument types the return type is
``double``.

Example::

    cr> select avg(position), kind from locations
    ... group by kind order by kind;
    +------+-------------+
    |  avg | kind        |
    +------+-------------+
    | 3.25 | Galaxy      |
    | 3.0  | Planet      |
    | 2.5  | Star System |
    +------+-------------+
    SELECT 3 rows in set (... sec)

The ``avg`` aggregation on the ``bigint`` column might result in a precision
error if sum of elements exceeds 2^53::

    cr> select avg(t.val) from
    ... (select unnest([9223372036854775807, 9223372036854775807]) as val) t;
    +-----------------------+
    |                   avg |
    +-----------------------+
    | 9.223372036854776e+18 |
    +-----------------------+
    SELECT 1 row in set (... sec)

To address the precision error of the avg aggregation, we cast the aggregation
column to the ``numeric`` data type::

    cr> select avg(t.val :: numeric) from
    ... (select unnest([9223372036854775807, 9223372036854775807]) as val) t;
    +---------------------+
    |                 avg |
    +---------------------+
    | 9223372036854775807 |
    +---------------------+
    SELECT 1 row in set (... sec)

.. _aggregation-avg-distinct:

``avg(DISTINCT column)``
~~~~~~~~~~~~~~~~~~~~~~~~

The ``avg`` aggregate function also supports the ``distinct`` keyword. This
keyword changes the behaviour of the function so that it will only average the
number of distinct values in this column that are not ``NULL``::

    cr> select
    ...   avg(distinct position) AS avg_pos,
    ...   count(*),
    ...   date
    ... from locations group by date
    ... order by 1 desc, count(*) desc;
    +---------+-------+---------------+
    | avg_pos | count |          date |
    +---------+-------+---------------+
    |     4.0 |     1 | 1367366400000 |
    |     3.6 |     8 | 1373932800000 |
    |     2.0 |     4 |  308534400000 |
    +---------+-------+---------------+
    SELECT 3 rows in set (... sec)

::

    cr> select avg(distinct position) AS avg_pos from locations;
    +---------+
    | avg_pos |
    +---------+
    |     3.5 |
    +---------+
    SELECT 1 row in set (... sec)


.. _aggregation-count:

``count(column)``
-----------------

In contrast to the :ref:`aggregation-count-star` function the ``count``
function used with a column name as parameter will return the number of rows
with a non-``NULL`` value in that column.

Example::

    cr> select count(name), count(*), date from locations group by date
    ... order by count(name) desc, count(*) desc;
    +-------+-------+---------------+
    | count | count |          date |
    +-------+-------+---------------+
    |     7 |     8 | 1373932800000 |
    |     4 |     4 |  308534400000 |
    |     1 |     1 | 1367366400000 |
    +-------+-------+---------------+
    SELECT 3 rows in set (... sec)


.. _aggregation-count-distinct:

``count(DISTINCT column)``
~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``count`` aggregate function also supports the ``distinct`` keyword. This
keyword changes the behaviour of the function so that it will only count the
number of distinct values in this column that are not ``NULL``::

    cr> select
    ...   count(distinct kind) AS num_kind,
    ...   count(*),
    ...   date
    ... from locations group by date
    ... order by num_kind, count(*) desc;
    +----------+-------+---------------+
    | num_kind | count |          date |
    +----------+-------+---------------+
    |        1 |     1 | 1367366400000 |
    |        3 |     8 | 1373932800000 |
    |        3 |     4 |  308534400000 |
    +----------+-------+---------------+
    SELECT 3 rows in set (... sec)

::

    cr> select count(distinct kind) AS num_kind from locations;
    +----------+
    | num_kind |
    +----------+
    |        3 |
    +----------+
    SELECT 1 row in set (... sec)

.. SEEALSO::

    :ref:`aggregation-hyperloglog-distinct` for an alternative that trades some
    accuracy for improved performance.


.. _aggregation-count-star:

``count(*)``
~~~~~~~~~~~~

This aggregate function simply returns the number of rows that match the query.

``count(columName)`` is also possible, but currently only works on a primary
key column. The semantics are the same.

The return value is always of type ``bigint``.

::

    cr> select count(*) from locations;
    +-------+
    | count |
    +-------+
    |    13 |
    +-------+
    SELECT 1 row in set (... sec)

``count(*)`` can also be used on group by queries::

    cr> select count(*), kind from locations group by kind order by kind asc;
    +-------+-------------+
    | count | kind        |
    +-------+-------------+
    | 4     | Galaxy      |
    | 5     | Planet      |
    | 4     | Star System |
    +-------+-------------+
    SELECT 3 rows in set (... sec)


.. _aggregation-geometric-mean:

``geometric_mean(column)``
--------------------------

The ``geometric_mean`` aggregate function computes the geometric mean, a mean
for positive numbers. For details see: `Geometric Mean`_.

``geometric mean`` is defined on all numeric types and on timestamp.
:ref:`NUMERIC values <type-numeric>` are automatically casted to
:ref:`DOUBLE PRECISION <type-double-precision>`. It always returns values of
``double precision``. If a value is negative, all values were null or we got no
value at all ``NULL`` is returned. If any of the aggregated values is ``0`` the
result will be ``0.0`` as well.

.. CAUTION::

    Due to java double precision arithmetic it is possible that any two
    executions of the aggregate function on the same data produce slightly
    differing results.

Example::

    cr> select geometric_mean(position), kind from locations
    ... group by kind order by kind;
    +--------------------+-------------+
    |     geometric_mean | kind        |
    +--------------------+-------------+
    | 2.6321480259049848 | Galaxy      |
    | 2.6051710846973517 | Planet      |
    | 2.213363839400643  | Star System |
    +--------------------+-------------+
    SELECT 3 rows in set (... sec)


.. _aggregation-hyperloglog-distinct:

``hyperloglog_distinct(column, [precision])``
---------------------------------------------

The ``hyperloglog_distinct`` aggregate function calculates an approximate count
of distinct non-null values using the `HyperLogLog++`_ algorithm.

The return value data type is always a ``bigint``.

The first argument can be a reference to a column of all
:ref:`data-types-primitive`. :ref:`data-types-container` and
:ref:`data-types-geo` are not supported.

The optional second argument defines the used ``precision`` for the
`HyperLogLog++`_ algorithm. This allows to trade memory for accuracy, valid
values are ``4`` to ``18``. A precision of ``4`` uses approximately ``16``
bytes of memory. Each increase in precision doubles the memory requirement. So
precision ``5`` uses approximately ``32`` bytes, up to ``262144`` bytes for
precision ``18``.

The default value for the ``precision`` which is used if the second argument is
left out is ``14``.


Examples::

    cr> select hyperloglog_distinct(position) from locations;
    +----------------------+
    | hyperloglog_distinct |
    +----------------------+
    | 6                    |
    +----------------------+
    SELECT 1 row in set (... sec)

::

    cr> select hyperloglog_distinct(position, 4) from locations;
    +----------------------+
    | hyperloglog_distinct |
    +----------------------+
    | 6                    |
    +----------------------+
    SELECT 1 row in set (... sec)


.. _aggregation-mean:

``mean(column)``
----------------

An alias for :ref:`aggregation-avg`.


.. _aggregation-min:

``min(column)``
---------------

The ``min`` aggregate function returns the smallest value in a column that is
not ``NULL``. Its single argument is a column name and its return value is
always of the type of that column.

Example::

    cr> select min(position), kind
    ... from locations
    ... where name not like 'North %'
    ... group by kind order by min(position) asc, kind asc;
    +-----+-------------+
    | min | kind        |
    +-----+-------------+
    | 1   | Planet      |
    | 1   | Star System |
    | 2   | Galaxy      |
    +-----+-------------+
    SELECT 3 rows in set (... sec)

::

    cr> select min(date) from locations;
    +--------------+
    | min          |
    +--------------+
    | 308534400000 |
    +--------------+
    SELECT 1 row in set (... sec)

``min`` returns ``NULL`` if the column does not contain any value but ``NULL``.
It is allowed on columns with primitive data types. On ``text`` columns it will
return the lexicographically smallest.

::

    cr> select min(name), kind from locations
    ... group by kind order by kind asc;
    +------------------------------------+-------------+
    | min                                | kind        |
    +------------------------------------+-------------+
    | Galactic Sector QQ7 Active J Gamma | Galaxy      |
    |                                    | Planet      |
    | Aldebaran                          | Star System |
    +------------------------------------+-------------+
    SELECT 3 rows in set (... sec)


.. _aggregation-max:

``max(column)``
---------------

It behaves exactly like ``min`` but returns the biggest value in a column that
is not ``NULL``.

Some Examples::

    cr> select max(position), kind from locations
    ... group by kind order by kind desc;
    +-----+-------------+
    | max | kind        |
    +-----+-------------+
    |   4 | Star System |
    |   5 | Planet      |
    |   6 | Galaxy      |
    +-----+-------------+
    SELECT 3 rows in set (... sec)

::

    cr> select max(position) from locations;
    +-----+
    | max |
    +-----+
    |   6 |
    +-----+
    SELECT 1 row in set (... sec)

::

    cr> select max(name), kind from locations
    ... group by kind order by max(name) desc;
    +-------------------+-------------+
    | max               | kind        |
    +-------------------+-------------+
    | Outer Eastern Rim | Galaxy      |
    | Bartledan         | Planet      |
    | Altair            | Star System |
    +-------------------+-------------+
    SELECT 3 rows in set (... sec)


.. _aggregation-max_by:

``max_by(returnField, searchField)``
------------------------------------

Returns the value of ``returnField`` where ``searchField`` has the highest
value.

If there are ties for ``searchField`` the result is non-deterministic and can be
any of the ``returnField`` values of the ties.

``NULL`` values in the ``searchField`` don't count as max but are skipped.


An Example::

    cr> SELECT max_by(mountain, height) FROM sys.summits;
    +------------+
    | max_by     |
    +------------+
    | Mont Blanc |
    +------------+
    SELECT 1 row in set (... sec)


.. _aggregation-min_by:

``min_by(returnField, searchField)``
------------------------------------


Returns the value of ``returnField`` where ``searchField`` has the lowest
value.

If there are ties for ``searchField`` the result is non-deterministic and can be
any of the ``returnField`` values of the ties.

``NULL`` values in the ``searchField`` don't count as min but are skipped.

An Example::

    cr> SELECT min_by(mountain, height) FROM sys.summits;
    +-------------+
    | min_by      |
    +-------------+
    | Puy de Rent |
    +-------------+
    SELECT 1 row in set (... sec)


.. _aggregation-stddev:

``stddev(column)``
------------------

``stddev`` is an alias for :ref:`aggregation-stddev-samp`.


.. _aggregation-stddev-pop:

``stddev_pop(column)``
----------------------

The ``stddev_pop`` aggregate function computes the `Population Standard Deviation`_
of the set of non-null values in a column. It is a measure of the variation
of data values. A low standard deviation indicates that the values tend to be
near the mean.

``stddev_pop`` is defined on all :ref:`numeric types<data-types-numeric>` and on
timestamp. Return value will be of type ``numeric`` with unspecified precision
and scale if the input value is of ``numeric`` type, and ``double precision``
for any other type. If all values were null or we got no value at all ``NULL``
is returned.

Example::

    cr> select stddev_pop(position), kind from locations
    ... group by kind order by kind;
    +--------------------+-------------+
    |         stddev_pop | kind        |
    +--------------------+-------------+
    | 1.920286436967152  | Galaxy      |
    | 1.4142135623730951 | Planet      |
    | 1.118033988749895  | Star System |
    +--------------------+-------------+
    SELECT 3 rows in set (... sec)

.. CAUTION::

    Due to Java double precision arithmetic it is possible that any two
    executions of the aggregate function on the same data produce slightly
    differing results.

.. _aggregation-stddev-samp:

``stddev_samp(column)``
-----------------------

The ``stddev_samp`` aggregate function computes the `Sample Standard Deviation`_
of the set of non-null values in a column. It is a measure of the variation
of data values. A low standard deviation indicates that the values tend to be
near the mean.

``stddev_samp`` is defined on all :ref:`numeric types<data-types-numeric>` and on
timestamp. Return value will be of type ``numeric`` with unspecified precision
and scale if the input value is of ``numeric`` type, and ``double precision``
for any other type. If all values were null or we got no value at all ``NULL``
is returned.

Example::

    cr> select stddev_samp(position), kind from locations
    ... group by kind order by kind;
    +--------------------+-------------+
    |        stddev_samp | kind        |
    +--------------------+-------------+
    | 2.217355782608345  | Galaxy      |
    | 1.5811388300841898 | Planet      |
    | 1.2909944487358056 | Star System |
    +--------------------+-------------+
    SELECT 3 rows in set (... sec)

.. CAUTION::

    Due to Java double precision arithmetic it is possible that any two
    executions of the aggregate function on the same data produce slightly
    differing results.

.. _aggregation-string-agg:

``string_agg(column, delimiter)``
---------------------------------

The ``string_agg`` aggregate function concatenates the input values into a
string, where each value is separated by a delimiter.

If all input values are null, null is returned as a result.


::

   cr> select string_agg(col1, ', ') from (values('a'), ('b'), ('c')) as t;
   +------------+
   | string_agg |
   +------------+
   | a, b, c    |
   +------------+
   SELECT 1 row in set (... sec)

.. SEEALSO::

    :ref:`aggregation-array-agg`


.. _aggregation-percentile:

``percentile(column, {fraction | fractions} [, compression])``
--------------------------------------------------------------

The ``percentile`` aggregate function computes a `Percentile`_ over numeric
non-null values in a column. Values of type :ref:`NUMERIC <type-numeric>` are
not supported.

Percentiles show the point at which a certain percentage of observed values
occur. For example, the 98th percentile is the value which is greater than 98%
of the observed values. The result is defined and computed as an interpolated
weighted average. According to that it allows the median of the input data to
be defined conveniently as the 50th percentile.

The :ref:`function <gloss-function>` expects a single fraction or an array of
fractions and a column name. Independent of the input column data type the
result of ``percentile`` always returns a ``double precision``. If the value at
the specified column is ``null`` the row is ignored. Fractions must be double
precision values between 0 and 1. When supplied a single fraction, the function
will return a single value corresponding to the percentile of the specified
fraction::

    cr> select percentile(position, 0.95), kind from locations
    ... group by kind order by kind;
    +------------+-------------+
    | percentile | kind        |
    +------------+-------------+
    |        6.0 | Galaxy      |
    |        5.0 | Planet      |
    |        4.0 | Star System |
    +------------+-------------+
    SELECT 3 rows in set (... sec)

When supplied an array of fractions, the function will return an array of
values corresponding to the percentile of each fraction specified::

    cr> select percentile(position, [0.0013, 0.9987]) as perc from locations;
    +------------+
    | perc       |
    +------------+
    | [1.0, 6.0] |
    +------------+
    SELECT 1 row in set (... sec)

When a query with ``percentile`` function won't match any rows then a null
result is returned.

To be able to calculate percentiles over a huge amount of data and to scale out
CrateDB calculates approximate instead of accurate percentiles. The algorithm
used by the percentile metric is called `TDigest`_. The accuracy/size trade-off
of the algorithm is defined by a single ``compression`` parameter which has a
default value of ``200.0``, but can be defined by passing in an optional 3rd
``double`` value argument as the ``compression``. However, there are a few
guidelines to keep in mind in this implementation:

- Extreme percentiles (e.g. 99%) are more accurate.
- For small sets, percentiles are highly accurate.
- It is difficult to generalize the exact level of accuracy, as it depends
  on your data distribution and volume of data being aggregated.
- The ``compression`` parameter is a trade-off between accuracy and memory
  usage. A higher value will result in more accurate percentiles but will
  consume more memory.


.. _aggregation-sum:

``sum(column)``
---------------

Returns the sum of a set of numeric input values that are not ``NULL``.
Depending on the argument type a suitable return type is chosen. For ``real``,
``double precision``, ``numeric`` and ``interval`` argument types, the return
type is the same as the argument type. For ``byte``, ``smallint``, ``integer``
and ``bigint`` the return type is always ``bigint``. If the range of ``bigint``
values (-2^64 to 2^64-1) gets exceeded an ``ArithmeticException`` will be
raised.

::

    cr> select sum(position), kind from locations
    ... group by kind order by sum(position) asc;
    +-----+-------------+
    | sum | kind        |
    +-----+-------------+
    |  10 | Star System |
    |  13 | Galaxy      |
    |  15 | Planet      |
    +-----+-------------+
    SELECT 3 rows in set (... sec)

::

    cr> select sum(position) as position_sum from locations;
    +--------------+
    | position_sum |
    +--------------+
    | 38           |
    +--------------+
    SELECT 1 row in set (... sec)

::

    cr> select sum(name), kind from locations group by kind order by sum(name) desc;
    SQLParseException[Cannot cast value `Aldebaran` to type `byte`]

If the ``sum`` aggregation on a numeric data type with the fixed length can
potentially exceed its range it is possible to handle the overflow by casting
the :ref:`function <gloss-function>` argument to the :ref:`numeric type
<type-numeric>` with an arbitrary precision.

.. Hidden: create user visits table

    cr> CREATE TABLE uservisits (id integer, count bigint)
    ... CLUSTERED INTO 1 SHARDS
    ... WITH (number_of_replicas = 0);
    CREATE OK, 1 row affected (... sec)

.. Hidden: insert into uservisits table

    cr> INSERT INTO uservisits VALUES (1, 9223372036854775806), (2, 10);
    INSERT OK, 2 rows affected  (... sec)

.. Hidden: refresh uservisits table

    cr> REFRESH TABLE uservisits;
    REFRESH OK, 1 row affected  (... sec)

The ``sum`` aggregation on the ``bigint`` column will result in an overflow
in the following aggregation query::

    cr> SELECT sum(count)
    ... FROM uservisits;
    ArithmeticException[long overflow]

To address the overflow of the sum aggregation on the given field, we cast
the aggregation column to the ``numeric`` data type::

    cr> SELECT sum(count::numeric)
    ... FROM uservisits;
    +---------------------+
    |                 sum |
    +---------------------+
    | 9223372036854775816 |
    +---------------------+
    SELECT 1 row in set (... sec)

.. Hidden: refresh uservisits table

    cr> DROP TABLE uservisits;
    DROP OK, 1 row affected (... sec)


.. _aggregation-variance:

``variance(column)``
--------------------

The ``variance`` aggregate function computes the `Variance`_ of the set of
non-null values in a column. It is a measure about how far a set of numbers is
spread. A variance of ``0.0`` indicates that all values are the same.

``variance`` is defined on all numeric types, except for
:ref:`NUMERIC <type-numeric>`, and on timestamp. It always returns a
``double precision`` value. If all values were null or we got no value at all
``NULL`` is returned.

Example::

    cr> select variance(position), kind from locations
    ... group by kind order by kind desc;
    +----------+-------------+
    | variance | kind        |
    +----------+-------------+
    |   1.25   | Star System |
    |   2.0    | Planet      |
    |   3.6875 | Galaxy      |
    +----------+-------------+
    SELECT 3 rows in set (... sec)

.. CAUTION::

    Due to java double precision arithmetic it is possible that any two
    executions of the aggregate function on the same data produce slightly
    differing results.

.. _aggregation-topk:

``topk(column, [k], [max_capacity])``
-------------------------------------


The ``topk`` aggregate function computes ``k`` most frequent values. The result
is an ``OBJECT`` in the following format::

    {
        "frequencies": [
            {
                "estimate": <estimated_frequency>,
                "item": <value_of_column>,
                "lower_bound": <lower_bound>,
                "upper_bound": <upper_bound>"
            },
            ...
        ],
        "maximum_error": <max_error>
    }

The ``frequencies`` list is ordered by the estimated frequency, with the most
common items listed first.

``k`` defaults to 8 and can't exceed 5000. The ``max_capacity`` parameter is
optional and describes the maximum number of tracked items and must be in the
power of 2 and defaults to 8192.

Example::

    cr> select topk(country, 3) from sys.summits;
    +------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | topk                                                                                                                                                                                                                                                             |
    +------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | {"frequencies": [{"estimate": 436, "item": "IT", "lower_bound": 436, "upper_bound": 436}, {"estimate": 401, "item": "AT", "lower_bound": 401, "upper_bound": 401}, {"estimate": 320, "item": "CH", "lower_bound": 320, "upper_bound": 320}], "maximum_error": 0} |
    +------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    SELECT 1 row in set (... sec)


Internally a `Frequency Sketch`_ is used to track the frequencies of the most
common values. Higher values in ``max_capacity`` provide better accuracy at the
cost of increased memory usage. If less different items than 75 % of the
``max_capacity`` are processed the frequencies of the result are exact, otherwise
they will be an approximation. The result contains all values with their
frequencies above the error threshold and may also include false positives.
The error threshold indicates the minimum frequency which can be detected
reliably and is defined as followed::

    M = max_capacity, always a power of 2
    N = Total counts of items
    e = Epsilon = 3.5/M (minimum detectable frequency)

    error threshold = (N < 0.75 * M)? 0 : e * N.

The following table is an
extract of the `Error Threshold Table`_ and shows the error threshold in relation
to the ``max_capacity`` and the number of processed items. A threshold of 0
indicates that the frequencies are exact.

.. list-table:: Error Threshold
   :widths: 20 20 20 20 20 20 20 20
   :header-rows: 1
   :stub-columns: 1

   * - max_capacity vs. items
     - 8192
     - 16384
     - 32768
     - 65536
     - 131072
     - 262144
     - 524288
   * - 10000
     - 4
     - 0
     - 0
     - 0
     - 0
     - 0
     - 0
   * - 100000
     - 43
     - 21
     - 11
     - 5
     - 3
     - 0
     - 0
   * - 1000000
     - 427
     - 214
     - 107
     - 53
     - 27
     - 13
     - 7
   * - 10000000
     - 4272
     - 2136
     - 1068
     - 534
     - 267
     - 134
     - 67
   * - 100000000
     - 42725
     - 21362
     - 10681
     - 5341
     - 2670
     - 1335
     - 668
   * - 1000000000
     - 427246
     - 213623
     - 106812
     - 53406
     - 26703
     - 13351
     - 6676


The error threshold shows which ranges of frequencies can be tracked depending
on the number of items and capacity. E.g. Processing 10,000 items with the
``max_capacity`` of 8192 indicates a error threshold of 4. Therefore all items
with frequencies greater 4 will be included. Some items with frequencies below
the threshold 4 may also appear in the result.

.. _aggregation-limitations:

Limitations
===========

- ``DISTINCT`` is not supported with aggregations on :ref:`sql_joins`.

- Aggregate functions can only be applied to columns with a :ref:`plain index
  <sql_ddl_index_plain>`, which is the default for all :ref:`primitive type
  <data-types-primitive>` columns.


.. _Aggregate function: https://en.wikipedia.org/wiki/Aggregate_function
.. _Geometric Mean: https://en.wikipedia.org/wiki/Geometric_mean
.. _HyperLogLog++: https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/40671.pdf
.. _Percentile: https://en.wikipedia.org/wiki/Percentile
.. _Population Standard Deviation: https://en.wikipedia.org/wiki/Standard_deviation
.. _Sample Standard Deviation: https://en.wikipedia.org/wiki/Standard_deviation#Corrected_sample_standard_deviation
.. _TDigest: https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf
.. _Variance: https://en.wikipedia.org/wiki/Variance
.. _Frequency Sketch: https://datasketches.apache.org/docs/Frequency/FrequencySketches.html
.. _Error Threshold Table: https://datasketches.apache.org/docs/Frequency/FrequentItemsErrorTable.html
