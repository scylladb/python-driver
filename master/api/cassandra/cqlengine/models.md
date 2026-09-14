# cassandra.cqlengine.models

Table models for object mapping

<a id="module-cassandra.cqlengine.models"></a>

## Model

### *class* cassandra.cqlengine.models.Model(\\\*\\\*kwargs)

The initializer creates an instance of the model. Pass in keyword arguments for columns you’ve defined on the model.

```python
class Person(Model):
    id = columns.UUID(primary_key=True)
    first_name  = columns.Text()
    last_name = columns.Text()

person = Person(first_name='Blake', last_name='Eggleston')
person.first_name  #returns 'Blake'
person.last_name  #returns 'Eggleston'
```

Model attributes define how the model maps to tables in the database. These are class variables that should be set
when defining Model deriviatives.

#### \_\_abstract_\_ *= False*

*Optional.* Indicates that this model is only intended to be used as a base class for other models.
You can’t create tables for abstract models, but checks around schema validity are skipped during class construction.

#### \_\_table_name_\_ *= None*

*Optional.* Sets the name of the CQL table for this model. If left blank, the table name will be the name of the model, with it’s module name as it’s prefix. Manually defined table names are not inherited.

#### \_\_table_name_case_sensitive_\_ *= False*

*Optional.* By default, \_\_table_name_\_ is case insensitive. Set this to True if you want to preserve the case sensitivity.

#### \_\_keyspace_\_ *= None*

Sets the name of the keyspace used by this model.

#### \_\_connection_\_ *= None*

Sets the name of the default connection used by this model.

#### \_\_default_ttl_\_ *= None*

Will be deprecated in release 4.0. You can set the default ttl by configuring the table `__options__`. See [Default TTL and Per Query TTL](https://python-driver.docs.scylladb.com/master/cqlengine/queryset.md#ttl-change) for more details.

#### \_\_discriminator_value_\_ *= None*

*Optional* Specifies a value for the discriminator column when using model inheritance.

See [Model Inheritance](https://python-driver.docs.scylladb.com/master/cqlengine/models.md#model-inheritance) for usage examples.

Each table can have its own set of configuration options, including compaction. Unspecified, these default to sensible values in
the server. To override defaults, set options using the model `__options__` attribute, which allows options specified a dict.

When a table is synced, it will be altered to match the options set on your table.
This means that if you are changing settings manually they will be changed back on resync.

Do not use the options settings of cqlengine if you want to manage your compaction settings manually.

See the [list of supported table properties for more information](http://www.datastax.com/documentation/cql/3.1/cql/cql_reference/tabProp.html).

#### \_\_options_\_

For example:

```python
class User(Model):
    __options__ = {'compaction': {'class': 'LeveledCompactionStrategy',
                                  'sstable_size_in_mb': '64',
                                  'tombstone_threshold': '.2'},
                   'comment': 'User data stored here'}

    user_id = columns.UUID(primary_key=True)
    name = columns.Text()
```

or :

```python
class TimeData(Model):
    __options__ = {'compaction': {'class': 'SizeTieredCompactionStrategy',
                                  'bucket_low': '.3',
                                  'bucket_high': '2',
                                  'min_threshold': '2',
                                  'max_threshold': '64',
                                  'tombstone_compaction_interval': '86400'},
                   'gc_grace_seconds': '0'}
```

#### \_\_compute_routing_key_\_ *= True*

*Optional* Setting False disables computing the routing key for TokenAwareRouting

The base methods allow creating, storing, and querying modeled objects.

#### *classmethod* create(\*\*kwargs)

Create an instance of this model in the database.

Takes the model column values as keyword arguments. Setting a value to
None is equivalent to running a CQL DELETE on that column.

Returns the instance.

#### if_not_exists()

Check the existence of an object before insertion. The existence of an
object is determined by its primary key(s). And please note using this flag
would incur performance cost.

If the insertion isn’t applied, a [`LWTException`](https://python-driver.docs.scylladb.com/master/api/cassandra/cqlengine/query.md#cassandra.cqlengine.query.LWTException) is raised.

```python
try:
    TestIfNotExistsModel.if_not_exists().create(id=id, count=9, text='111111111111')
except LWTException as e:
    # handle failure case
    print(e.existing)  # dict containing LWT result fields)
```

This method is supported on Cassandra 2.0 or later.

#### if_exists()

Check the existence of an object before an update or delete. The existence of an
object is determined by its primary key(s). And please note using this flag
would incur performance cost.

If the update or delete isn’t applied, a [`LWTException`](https://python-driver.docs.scylladb.com/master/api/cassandra/cqlengine/query.md#cassandra.cqlengine.query.LWTException) is raised.

```python
try:
    TestIfExistsModel.objects(id=id).if_exists().update(count=9, text='111111111111')
except LWTException as e:
    # handle failure case
    pass
```

This method is supported on Cassandra 2.0 or later.

#### save()

Saves an object to the database.

```python
#create a person instance
person = Person(first_name='Kimberly', last_name='Eggleston')
#saves it to Cassandra
person.save()
```

#### update(\*\*values)

Performs an update on the model instance. You can pass in values to set on the model
for updating, or you can call without values to execute an update against any modified
fields. If no fields on the model have been modified since loading, no query will be
performed. Model validation is performed normally. Setting a value to None is
equivalent to running a CQL DELETE on that column.

It is possible to do a blind update, that is, to update a field without having first selected the object out of the database.
See [Blind Updates](https://python-driver.docs.scylladb.com/master/api/cassandra/cqlengine/query.md#blind-updates)

#### iff(\*\*values)

Checks to ensure that the values specified are correct on the Cassandra cluster.
Simply specify the column(s) and the expected value(s).  As with if_not_exists,
this incurs a performance cost.

If the insertion isn’t applied, a [`LWTException`](https://python-driver.docs.scylladb.com/master/api/cassandra/cqlengine/query.md#cassandra.cqlengine.query.LWTException) is raised.

```python
t = TestTransactionModel(text='some text', count=5)
try:
     t.iff(count=5).update('other text')
except LWTException as e:
    # handle failure case
    print(e.existing) # existing object
```

#### *classmethod* get(\*args, \*\*kwargs)

Returns a single object based on the passed filter constraints.

This is a pass-through to the model objects().:method:~cqlengine.queries.get.

#### *classmethod* filter(\*args, \*\*kwargs)

Returns a queryset based on filter parameters.

This is a pass-through to the model objects().:method:~cqlengine.queries.filter.

#### *classmethod* all()

Returns a queryset representing all stored objects

This is a pass-through to the model objects().all()

#### delete()

Deletes the object from the database

#### batch(batch_object)

Sets the batch object to run instance updates and inserts queries with.

See [Batch Queries](https://python-driver.docs.scylladb.com/master/cqlengine/batches.md) for usage examples

#### timeout(timeout)

Sets a timeout for use in [`save()`](#cassandra.cqlengine.models.Model.save), [`update()`](#cassandra.cqlengine.models.Model.update), and [`delete()`](#cassandra.cqlengine.models.Model.delete)
operations

#### timestamp(timedelta_or_datetime)

Sets the timestamp for the query

#### ttl(ttl_in_sec)

Sets the ttl values to run instance updates and inserts queries with.

#### using(connection=None)

Change the context on the fly of the model instance (keyspace, connection)

#### *classmethod* column_family_name(include_keyspace=True)

Returns the column family name if it’s been defined
otherwise, it creates it from the module and class name

Models also support dict-like access:

#### len(m)

Returns the number of columns defined in the model

#### m()

Returns the value of column `col_name`

### m[col_name] = value

Set `m[col_name]` to value

#### keys()

Returns a list of column IDs.

#### values()

Returns list of column values.

#### items()

Returns a list of column ID/value tuples.
