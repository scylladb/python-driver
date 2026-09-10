# cassandra.cqlengine.columns

Column types for object mapping models

<a id="module-cassandra.cqlengine.columns"></a>

## Columns

Columns in your models map to columns in your CQL table. You define CQL columns by defining column attributes on your model classes.
For a model to be valid it needs at least one primary key column and one non-primary key column.

Just as in CQL, the order you define your columns in is important, and is the same order they are defined in on a model’s corresponding table.

Each column on your model definitions needs to be an instance of a Column class.

### *class* cassandra.cqlengine.columns.Column(\*\*kwargs)

#### primary_key *= False*

bool flag, indicates this column is a primary key. The first primary key defined
on a model is the partition key (unless partition keys are set), all others are cluster keys

#### partition_key *= False*

indicates that this column should be the partition key, defining
more than one partition key column creates a compound partition key

#### index *= False*

bool flag, indicates an index should be created for this column

#### custom_index *= False*

bool flag, indicates an index is managed outside of cqlengine. This is
useful if you want to do filter queries on fields that have custom
indexes.

#### db_field *= None*

the fieldname this field will map to in the database

#### default *= None*

the default value, can be a value or a callable (no args)

#### required *= False*

boolean, is the field required? Model validation will raise and
exception if required is set to True and there is a None value assigned

#### clustering_order *= None*

only applicable on clustering keys (primary keys that are not partition keys)
determines the order that the clustering keys are sorted on disk

#### discriminator_column *= False*

boolean, if set to True, this column will be used for discriminating records
of inherited models.

Should only be set on a column of an abstract model being used for inheritance.

There may only be one discriminator column per model. See [`__discriminator_value__`](https://python-driver.docs.scylladb.com/stable/api/cassandra/cqlengine/models.md#cassandra.cqlengine.models.Model.__discriminator_value__)
for how to specify the value of this column on specialized models.

#### static *= False*

boolean, if set to True, this is a static column, with a single value per partition

## Column Types

Columns of all types are initialized by passing [`Column`](#cassandra.cqlengine.columns.Column) attributes to the constructor by keyword.

### *class* cassandra.cqlengine.columns.Ascii(\*\*kwargs)

Stores a US-ASCII character string

* **Parameters:**
  * **min_length** (*int*) – Sets the minimum length of this string, for validation purposes.
    Defaults to 1 if this is a `required` column. Otherwise, None.
  * **max_length** (*int*) – Sets the maximum length of this string, for validation purposes.

### *class* cassandra.cqlengine.columns.BigInt(\*\*kwargs)

Stores a 64-bit signed integer value

### *class* cassandra.cqlengine.columns.Blob(\*\*kwargs)

Stores a raw binary value

### cassandra.cqlengine.columns.Bytes

alias of [`Blob`](#cassandra.cqlengine.columns.Blob)

### *class* cassandra.cqlengine.columns.Boolean(\*\*kwargs)

Stores a boolean True or False value

### *class* cassandra.cqlengine.columns.Counter(index=False, db_field=None, required=False)

Stores a counter that can be incremented and decremented

### *class* cassandra.cqlengine.columns.Date(\*\*kwargs)

Stores a simple date, with no time-of-day

#### Versionchanged
Changed in version 2.6.0: removed overload of Date and DateTime. DateTime is a drop-in replacement for legacy models

requires C\* 2.2+ and protocol v4+

### *class* cassandra.cqlengine.columns.DateTime(\*\*kwargs)

Stores a datetime value

#### truncate_microseconds *= False*

Set this `True` to have model instances truncate the date, quantizing it in the same way it will be in the database.
This allows equality comparison between assigned values and values read back from the database:

```default
DateTime.truncate_microseconds = True
assert Model.create(id=0, d=datetime.utcnow()) == Model.objects(id=0).first()
```

Defaults to `False` to preserve legacy behavior. May change in the future.

### *class* cassandra.cqlengine.columns.Decimal(\*\*kwargs)

Stores a variable precision decimal value

### *class* cassandra.cqlengine.columns.Double(\*\*kwargs)

Stores a double-precision floating-point value

### *class* cassandra.cqlengine.columns.Float(primary_key=False, partition_key=False, index=False, db_field=None, default=None, required=False, clustering_order=None, discriminator_column=False, static=False, custom_index=False)

Stores a single-precision floating-point value

### *class* cassandra.cqlengine.columns.Integer(\*\*kwargs)

Stores a 32-bit signed integer value

### *class* cassandra.cqlengine.columns.List(value_type, default=<class 'list'>, frozen=False, \*\*kwargs)

Stores a list of ordered values

[http://www.datastax.com/documentation/cql/3.1/cql/cql_using/use_list_t.html](http://www.datastax.com/documentation/cql/3.1/cql/cql_using/use_list_t.html)

* **Parameters:**
  * **value_type** – a column class indicating the types of the value
  * **frozen** – if True, the collection will be frozen (immutable) and
    use FULL indexes instead of VALUES indexes

### *class* cassandra.cqlengine.columns.Map(key_type, value_type, default=<class 'dict'>, frozen=False, \*\*kwargs)

Stores a key -> value map (dictionary)

[https://docs.datastax.com/en/dse/6.7/cql/cql/cql_using/useMap.html](https://docs.datastax.com/en/dse/6.7/cql/cql/cql_using/useMap.html)

* **Parameters:**
  * **key_type** – a column class indicating the types of the key
  * **value_type** – a column class indicating the types of the value
  * **frozen** – if True, the collection will be frozen (immutable) and
    use FULL indexes instead of VALUES indexes

### *class* cassandra.cqlengine.columns.Set(value_type, strict=True, default=<class 'set'>, frozen=False, \*\*kwargs)

Stores a set of unordered, unique values

[http://www.datastax.com/documentation/cql/3.1/cql/cql_using/use_set_t.html](http://www.datastax.com/documentation/cql/3.1/cql/cql_using/use_set_t.html)

* **Parameters:**
  * **value_type** – a column class indicating the types of the value
  * **strict** – sets whether non set values will be coerced to set
    type on validation, or raise a validation error, defaults to True
  * **frozen** – if True, the collection will be frozen (immutable) and
    use FULL indexes instead of VALUES indexes

### *class* cassandra.cqlengine.columns.SmallInt(\*\*kwargs)

Stores a 16-bit signed integer value

#### Versionadded
Added in version 2.6.0.

requires C\* 2.2+ and protocol v4+

### *class* cassandra.cqlengine.columns.Text(min_length=None, max_length=None, \*\*kwargs)

Stores a UTF-8 encoded string

* **Parameters:**
  * **min_length** (*int*) – Sets the minimum length of this string, for validation purposes.
    Defaults to 1 if this is a `required` column. Otherwise, None.
  * **max_length** (*int*) – Sets the maximum length of this string, for validation purposes.

### *class* cassandra.cqlengine.columns.Time(\*\*kwargs)

Stores a timezone-naive time-of-day, with nanosecond precision

#### Versionadded
Added in version 2.6.0.

requires C\* 2.2+ and protocol v4+

### *class* cassandra.cqlengine.columns.TimeUUID(\*\*kwargs)

UUID containing timestamp

### *class* cassandra.cqlengine.columns.TinyInt(\*\*kwargs)

Stores an 8-bit signed integer value

#### Versionadded
Added in version 2.6.0.

requires C\* 2.2+ and protocol v4+

### *class* cassandra.cqlengine.columns.UserDefinedType(user_type, \*\*kwargs)

User Defined Type column

[http://www.datastax.com/documentation/cql/3.1/cql/cql_using/cqlUseUDT.html](http://www.datastax.com/documentation/cql/3.1/cql/cql_using/cqlUseUDT.html)

These columns are represented by a specialization of [`cassandra.cqlengine.usertype.UserType`](https://python-driver.docs.scylladb.com/stable/api/cassandra/cqlengine/usertype.md#cassandra.cqlengine.usertype.UserType).

Please see [User Defined Types](https://python-driver.docs.scylladb.com/stable/cqlengine/models.md#user-types) for examples and discussion.

* **Parameters:**
  **user_type** (*type*) – specifies the [`UserType`](https://python-driver.docs.scylladb.com/stable/api/cassandra/cqlengine/usertype.md#cassandra.cqlengine.usertype.UserType) model of the column

### *class* cassandra.cqlengine.columns.UUID(\*\*kwargs)

Stores a type 1 or 4 UUID

### *class* cassandra.cqlengine.columns.VarInt(\*\*kwargs)

Stores an arbitrary-precision integer
