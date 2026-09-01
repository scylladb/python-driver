# cassandra.util

Utilities

<a id="module-cassandra.util"></a>

### cassandra.util.datetime_from_timestamp(timestamp)

Creates a timezone-agnostic datetime from timestamp (in seconds) in a consistent manner.
Works around a Windows issue with large negative timestamps (PYTHON-119),
and rounding differences in Python 3.4 (PYTHON-340).

* **Parameters:**
  **timestamp** – a unix timestamp, in seconds

### cassandra.util.datetime_from_ms_timestamp(timestamp_ms)

Creates a timezone-agnostic datetime from a timestamp in milliseconds,
using integer arithmetic to preserve precision for large values.

* **Parameters:**
  **timestamp_ms** – a unix timestamp, in milliseconds (integer)

### cassandra.util.utc_datetime_from_ms_timestamp(timestamp)

Creates a UTC datetime from a timestamp in milliseconds. See
[`datetime_from_timestamp()`](#cassandra.util.datetime_from_timestamp).

Raises an OverflowError if the timestamp is out of range for
`datetime`.

* **Parameters:**
  **timestamp** – timestamp, in milliseconds

### cassandra.util.ms_timestamp_from_datetime(dt)

Converts a datetime to a timestamp expressed in milliseconds.

* **Parameters:**
  **dt** – a `datetime.datetime`

### cassandra.util.unix_time_from_uuid1(uuid_arg)

Converts a version 1 `uuid.UUID` to a timestamp with the same precision
as `time.time()` returns.  This is useful for examining the
results of queries returning a v1 `UUID`.

* **Parameters:**
  **uuid_arg** – a version 1 `UUID`

### cassandra.util.datetime_from_uuid1(uuid_arg)

Creates a timezone-agnostic datetime from the timestamp in the
specified type-1 UUID.

* **Parameters:**
  **uuid_arg** – a version 1 `UUID`

### cassandra.util.min_uuid_from_time(timestamp)

Generates the minimum TimeUUID (type 1) for a given timestamp, as compared by Cassandra.

See [`uuid_from_time()`](#cassandra.util.uuid_from_time) for argument and return types.

### cassandra.util.max_uuid_from_time(timestamp)

Generates the maximum TimeUUID (type 1) for a given timestamp, as compared by Cassandra.

See [`uuid_from_time()`](#cassandra.util.uuid_from_time) for argument and return types.

### cassandra.util.uuid_from_time(time_arg, node=None, clock_seq=None)

Converts a datetime or timestamp to a type 1 `uuid.UUID`.

* **Parameters:**
  * **time_arg** – The time to use for the timestamp portion of the UUID.
    This can either be a `datetime` object or a timestamp
    in seconds (as returned from `time.time()`).
  * **node** (*long*) – None integer for the UUID (up to 48 bits). If not specified, this
    field is randomized.
  * **clock_seq** (*int*) – Clock sequence field for the UUID (up to 14 bits). If not specified,
    a random sequence is generated.
* **Return type:**
  `uuid.UUID`

### cassandra.util.LOWEST_TIME_UUID *= UUID('00000000-0000-1000-8080-808080808080')*

The lowest possible TimeUUID, as sorted by Cassandra.

### cassandra.util.HIGHEST_TIME_UUID *= UUID('ffffffff-ffff-1fff-bf7f-7f7f7f7f7f7f')*

The highest possible TimeUUID, as sorted by Cassandra.

### *class* cassandra.util.SortedSet(iterable=())

A sorted set based on sorted list

A sorted set implementation is used in this case because it does not
require its elements to be immutable/hashable.

#Not implemented: update functions, inplace operators

### cassandra.util.sortedset

alias of [`SortedSet`](#cassandra.util.SortedSet)

### *class* cassandra.util.OrderedMap(\*args, \*\*kwargs)

An ordered map that accepts non-hashable types for keys. It also maintains the
insertion order of items, behaving as OrderedDict in that regard. These maps
are constructed and read just as normal mapping types, except that they may
contain arbitrary collections and other non-hashable items as keys:

```default
>>> od = OrderedMap([({'one': 1, 'two': 2}, 'value'),
...                  ({'three': 3, 'four': 4}, 'value2')])
>>> list(od.keys())
[{'two': 2, 'one': 1}, {'three': 3, 'four': 4}]
>>> list(od.values())
['value', 'value2']
```

These constructs are needed to support nested collections in Cassandra 2.1.3+,
where frozen collections can be specified as parameters to others:

```default
CREATE TABLE example (
    ...
    value map<frozen<map<int, int>>, double>
    ...
)
```

This class derives from the (immutable) Mapping API. Objects in these maps
are not intended be modified.

### *class* cassandra.util.OrderedMapSerializedKey(cass_type, protocol_version)

### *class* cassandra.util.Time(value)

Idealized time, independent of day.

Up to nanosecond resolution

Initializer value can be:

- integer_type: absolute nanoseconds in the day
- datetime.time: built-in time
- string_type: a string time of the form “HH:MM:SS[.mmmuuunnn]”

#### *property* hour

The hour component of this time (0-23)

#### *property* minute

The minute component of this time (0-59)

#### *property* second

The second component of this time (0-59)

#### *property* nanosecond

The fractional seconds component of the time, in nanoseconds

#### time()

Return a built-in datetime.time (nanosecond precision truncated to micros).

### *class* cassandra.util.Date(value)

Idealized date: year, month, day

Offers wider year range than datetime.date. For Dates that cannot be represented
as a datetime.date (because datetime.MINYEAR, datetime.MAXYEAR), this type falls back
to printing days_from_epoch offset.

Initializer value can be:

- integer_type: absolute days from epoch (1970, 1, 1). Can be negative.
- datetime.date: built-in date
- string_type: a string time of the form “yyyy-mm-dd”

#### *property* seconds

Absolute seconds from epoch (can be negative)

#### date()

Return a built-in datetime.date for Dates falling in the years [datetime.MINYEAR, datetime.MAXYEAR]

ValueError is raised for Dates outside this range.

### *class* cassandra.util.Point(x=nan, y=nan)

Represents a point geometry for DSE

#### x *= None*

x coordinate of the point

#### y *= None*

y coordinate of the point

#### *static* from_wkt(s)

Parse a Point geometry from a wkt string and return a new Point object.

### *class* cassandra.util.LineString(coords=())

Represents a linestring geometry for DSE

‘coords\`: a sequence of (x, y) coordinates of points in the linestring

#### coords *= None*

Tuple of (x, y) coordinates in the linestring

#### *static* from_wkt(s)

Parse a LineString geometry from a wkt string and return a new LineString object.

### *class* cassandra.util.Polygon(exterior=(), interiors=None)

Represents a polygon geometry for DSE

‘exterior\`: a sequence of (x, y) coordinates of points in the linestring
interiors: None, or a sequence of sequences or (x, y) coordinates of points describing interior linear rings

#### exterior *= None*

\_LinearRing representing the exterior of the polygon

#### interiors *= None*

Tuple of \_LinearRings representing interior holes in the polygon

#### *static* from_wkt(s)

Parse a Polygon geometry from a wkt string and return a new Polygon object.

### *class* cassandra.util.Distance(x=nan, y=nan, radius=nan)

Represents a Distance geometry for DSE

#### x *= None*

x coordinate of the center point

#### y *= None*

y coordinate of the center point

#### radius *= None*

radius to represent the distance from the center point

#### *static* from_wkt(s)

Parse a Distance geometry from a wkt string and return a new Distance object.

### *class* cassandra.util.Duration(months=0, days=0, nanoseconds=0)

Cassandra Duration Type

#### months *= 0*

#### days *= 0*

#### nanoseconds *= 0*

### *class* cassandra.util.DateRangePrecision

An “enum” representing the valid values for `DateRange.precision`.

#### YEAR *= 'YEAR'*

#### MONTH *= 'MONTH'*

#### DAY *= 'DAY'*

#### HOUR *= 'HOUR'*

#### MINUTE *= 'MINUTE'*

#### SECOND *= 'SECOND'*

#### MILLISECOND *= 'MILLISECOND'*

#### PRECISIONS *= ('YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND', 'MILLISECOND')*

### *class* cassandra.util.DateRangeBound(value, precision)

Represents a single date value and its precision for [`DateRange`](#cassandra.util.DateRange).

#### milliseconds

Integer representing milliseconds since the UNIX epoch. May be negative.

#### precision

String representing the precision of a bound. Must be a valid
[`DateRangePrecision`](#cassandra.util.DateRangePrecision) member.

[`DateRangeBound`](#cassandra.util.DateRangeBound) uses a millisecond offset from the UNIX epoch to
allow [`DateRange`](#cassandra.util.DateRange) to represent values datetime.datetime cannot.
For such values, string representions will show this offset rather than the
CQL representation.

* **Parameters:**
  * **value** – a value representing ms since the epoch. Accepts an
    integer or a datetime.
  * **precision** – a string representing precision

#### datetime()

Return [`milliseconds`](#cassandra.util.DateRangeBound.milliseconds) as a `datetime.datetime` if possible.
Raises an OverflowError if the value is out of range.

#### *classmethod* from_value(value)

Construct a new [`DateRangeBound`](#cassandra.util.DateRangeBound) from a given value. If
possible, use the value[‘milliseconds’] and value[‘precision’] keys
of the argument. Otherwise, use the argument as a (milliseconds,
precision) iterable.

* **Parameters:**
  **value** – a dictlike or iterable object

### cassandra.util.OPEN_BOUND *= DateRangeBound(milliseconds=None, precision=None)*

Represents \*, an open value or bound for [`DateRange`](#cassandra.util.DateRange).

### *class* cassandra.util.DateRange(lower_bound=None, upper_bound=None, value=None)

DSE DateRange Type

#### lower_bound

[`DateRangeBound`](#cassandra.util.DateRangeBound) representing the lower bound of a bounded range.

#### upper_bound

[`DateRangeBound`](#cassandra.util.DateRangeBound) representing the upper bound of a bounded range.

#### value

[`DateRangeBound`](#cassandra.util.DateRangeBound) representing the value of a single-value range.

As noted in its documentation, [`DateRangeBound`](#cassandra.util.DateRangeBound) uses a millisecond
offset from the UNIX epoch to allow [`DateRange`](#cassandra.util.DateRange) to represent values
datetime.datetime cannot. For such values, string representions will show
this offset rather than the CQL representation.

* **Parameters:**
  * **lower_bound** – a [`DateRangeBound`](#cassandra.util.DateRangeBound) or object accepted by
    [`DateRangeBound.from_value()`](#cassandra.util.DateRangeBound.from_value) to be used as a
    [`lower_bound`](#cassandra.util.DateRange.lower_bound). Mutually exclusive with value. If
    upper_bound is specified and this is not, the [`lower_bound`](#cassandra.util.DateRange.lower_bound)
    will be open.
  * **upper_bound** – a [`DateRangeBound`](#cassandra.util.DateRangeBound) or object accepted by
    [`DateRangeBound.from_value()`](#cassandra.util.DateRangeBound.from_value) to be used as a
    [`upper_bound`](#cassandra.util.DateRange.upper_bound). Mutually exclusive with value. If
    lower_bound is specified and this is not, the [`upper_bound`](#cassandra.util.DateRange.upper_bound)
    will be open.
  * **value** – a [`DateRangeBound`](#cassandra.util.DateRangeBound) or object accepted by
    [`DateRangeBound.from_value()`](#cassandra.util.DateRangeBound.from_value) to be used as [`value`](#cassandra.util.DateRange.value). Mutually
    exclusive with lower_bound and lower_bound.

### *class* cassandra.util.Version(version)

Internal minimalist class to compare versions.
A valid version is: <int>.<int>.<int>.<int or str>.

TODO: when python2 support is removed, use packaging.version.
