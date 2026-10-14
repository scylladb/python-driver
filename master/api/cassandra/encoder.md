# cassandra.encoder

Encoders for non-prepared Statements

<a id="module-cassandra.encoder"></a>

### *class* cassandra.encoder.Encoder

A container for mapping python types to CQL string literals when working
with non-prepared statements.  The type [`mapping`](#cassandra.encoder.Encoder.mapping) can be
directly customized by users.

#### mapping *= None*

A map of python types to encoder functions.

#### cql_encode_none()

Converts `None` to the string ‘NULL’.

#### cql_encode_object()

Default encoder for all objects that do not have a specific encoder function
registered. This function simply calls `str()` on the object.

#### cql_encode_all_types()

Converts any type into a CQL string, defaulting to `cql_encode_object`
if [`mapping`](#cassandra.encoder.Encoder.mapping) does not contain an entry for the type.

#### cql_encode_sequence()

Converts a sequence to a string of the form `(item1, item2, ...)`.  This
is suitable for `IN` value lists.

#### cql_encode_str()

Escapes quotes in `str` objects.

#### cql_encode_unicode()

Converts `unicode` objects to UTF-8 encoded strings with quote escaping.

#### cql_encode_bytes()

Converts strings, buffers, and bytearrays into CQL blob literals.

#### cql_encode_datetime()

Converts a `datetime.datetime` object to a (string) integer timestamp
with millisecond precision.

#### cql_encode_date()

Converts a `datetime.date` object to a string with format
`YYYY-MM-DD`.

#### cql_encode_map_collection()

Converts a dict into a string of the form `{key1: val1, key2: val2, ...}`.
This is suitable for `map` type columns.

#### cql_encode_list_collection()

Converts a sequence to a string of the form `[item1, item2, ...]`.  This
is suitable for `list` type columns.

#### cql_encode_set_collection()

Converts a sequence to a string of the form `{item1, item2, ...}`.  This
is suitable for `set` type columns.

#### cql_encode_tuple()

Converts a sequence to a string of the form `(item1, item2, ...)`.  This
is suitable for `tuple` type columns.
