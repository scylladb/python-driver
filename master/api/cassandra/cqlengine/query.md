# cassandra.cqlengine.query

Query and filter model objects

<a id="module-cassandra.cqlengine.query"></a>

## QuerySet

QuerySet objects are typically obtained by calling `objects()` on a model class.
The methods here are used to filter, order, and constrain results.

### *class* cassandra.cqlengine.query.ModelQuerySet(model)

#### all()

Returns a queryset matching all rows

```python
for user in User.objects().all():
    print(user)
```

#### batch(batch_obj)

Set a batch object to run the query on.

Note: running a select query with a batch object will raise an exception

#### consistency(consistency)

Sets the consistency level for the operation. See [`ConsistencyLevel`](https://python-driver.docs.scylladb.com/master/api/cassandra.md#cassandra.ConsistencyLevel).

```python
for user in User.objects(id=3).consistency(CL.ONE):
    print(user)
```

#### count()

Returns the number of rows matched by this query.

*Note: This function executes a SELECT COUNT() and has a performance cost on large datasets*

#### len(queryset)

Returns the number of rows matched by this query. This function uses [`count()`](#cassandra.cqlengine.query.ModelQuerySet.count) internally.

*Note: This function executes a SELECT COUNT() and has a performance cost on large datasets*

#### distinct(distinct_fields=None)

Returns the DISTINCT rows matched by this query.

distinct_fields default to the partition key fields if not specified.

*Note: distinct_fields must be a partition key or a static column*

```python
class Automobile(Model):
    manufacturer = columns.Text(partition_key=True)
    year = columns.Integer(primary_key=True)
    model = columns.Text(primary_key=True)
    price = columns.Decimal()

sync_table(Automobile)

# create rows

Automobile.objects.distinct()

# or

Automobile.objects.distinct(['manufacturer'])
```

#### filter(\*args, \*\*kwargs)

Adds WHERE arguments to the queryset, returning a new queryset

See [Retrieving objects with filters](https://python-driver.docs.scylladb.com/master/cqlengine/queryset.md#retrieving-objects-with-filters)

Returns a QuerySet filtered on the keyword arguments

#### get(\*args, \*\*kwargs)

Returns a single instance matching this query, optionally with additional filter kwargs.

See [Retrieving objects with filters](https://python-driver.docs.scylladb.com/master/cqlengine/queryset.md#retrieving-objects-with-filters)

Returns a single object matching the QuerySet.

```python
user = User.get(id=1)
```

If no objects are matched, a [`DoesNotExist`](#cassandra.cqlengine.query.DoesNotExist) exception is raised.

If more than one object is found, a [`MultipleObjectsReturned`](#cassandra.cqlengine.query.MultipleObjectsReturned) exception is raised.

#### limit(v)

Limits the number of results returned by Cassandra. Use *0* or *None* to disable.

*Note that CQL’s default limit is 10,000, so all queries without a limit set explicitly will have an implicit limit of 10,000*

```python
# Fetch 100 users
for user in User.objects().limit(100):
    print(user)

# Fetch all users
for user in User.objects().limit(None):
    print(user)
```

#### fetch_size(v)

Sets the number of rows that are fetched at a time.

*Note that driver’s default fetch size is 5000.*

```python
for user in User.objects().fetch_size(500):
    print(user)
```

#### if_not_exists()

Check the existence of an object before insertion.

If the insertion isn’t applied, a LWTException is raised.

#### if_exists()

Check the existence of an object before an update or delete.

If the update or delete isn’t applied, a LWTException is raised.

#### order_by(\*colnames)

Sets the column(s) to be used for ordering

Default order is ascending, prepend a ‘-’ to any column name for descending

*Note: column names must be a clustering key*

```python
from uuid import uuid1,uuid4

class Comment(Model):
    photo_id = UUID(primary_key=True)
    comment_id = TimeUUID(primary_key=True, default=uuid1) # second primary key component is a clustering key
    comment = Text()

sync_table(Comment)

u = uuid4()
for x in range(5):
    Comment.create(photo_id=u, comment="test %d" % x)

print("Normal")
for comment in Comment.objects(photo_id=u):
    print(comment.comment_id)

print("Reversed")
for comment in Comment.objects(photo_id=u).order_by("-comment_id"):
    print(comment.comment_id)
```

#### allow_filtering()

Enables the (usually) unwise practive of querying on a clustering key without also defining a partition key

#### only(fields)

Load only these fields for the returned query

#### defer(fields)

Don’t load these fields for the returned query

#### timestamp(timestamp)

Allows for custom timestamps to be saved with the record.

#### ttl(ttl)

Sets the ttl (in seconds) for modified data.

*Note that running a select query with a ttl value will raise an exception*

#### using(keyspace=None, connection=None)

Change the context on-the-fly of the Model class (keyspace, connection)

<a id="blind-updates"></a>

#### update(\*\*values)

Performs an update on the row selected by the queryset. Include values to update in the
update like so:

```python
Model.objects(key=n).update(value='x')
```

Passing in updates for columns which are not part of the model will raise a ValidationError.

Per column validation will be performed, but instance level validation will not
(i.e., Model.validate is not called).  This is sometimes referred to as a blind update.

For example:

```python
class User(Model):
    id = Integer(primary_key=True)
    name = Text()

setup(["localhost"], "test")
sync_table(User)

u = User.create(id=1, name="jon")

User.objects(id=1).update(name="Steve")

# sets name to null
User.objects(id=1).update(name=None)
```

Also supported is blindly adding and removing elements from container columns,
without loading a model instance from Cassandra.

Using the syntax .update(column_name={x, y, z}) will overwrite the contents of the container, like updating a
non container column. However, adding \_\_<operation> to the end of the keyword arg, makes the update call add
or remove items from the collection, without overwriting then entire column.

Given the model below, here are the operations that can be performed on the different container columns:

```python
class Row(Model):
    row_id      = columns.Integer(primary_key=True)
    set_column  = columns.Set(Integer)
    list_column = columns.List(Integer)
    map_column  = columns.Map(Integer, Integer)
```

`Set`

- add: adds the elements of the given set to the column
- remove: removes the elements of the given set to the column

```python
# add elements to a set
Row.objects(row_id=5).update(set_column__add={6})

# remove elements to a set
Row.objects(row_id=5).update(set_column__remove={4})
```

`List`

- append: appends the elements of the given list to the end of the column
- prepend: prepends the elements of the given list to the beginning of the column

```python
# append items to a list
Row.objects(row_id=5).update(list_column__append=[6, 7])

# prepend items to a list
Row.objects(row_id=5).update(list_column__prepend=[1, 2])
```

`Map`

- update: adds the given keys/values to the columns, creating new entries if they didn’t exist, and overwriting old ones if they did

```python
# add items to a map
Row.objects(row_id=5).update(map_column__update={1: 2, 3: 4})

# remove items from a map
Row.objects(row_id=5).update(map_column__remove={1, 2})
```

### *class* cassandra.cqlengine.query.BatchQuery(batch_type=None, timestamp=None, consistency=None, execute_on_exception=False, timeout=<object object>, connection=None)

Handles the batching of queries

[http://docs.datastax.com/en/cql/3.0/cql/cql_reference/batch_r.html](http://docs.datastax.com/en/cql/3.0/cql/cql_reference/batch_r.html)

See [Batch Queries](https://python-driver.docs.scylladb.com/master/cqlengine/batches.md) for more details.

* **Parameters:**
  * **batch_type** ([*BatchType*](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.BatchType) *,* *str* *or* *None*) – (optional) One of batch type values available through BatchType enum
  * **timestamp** ([*datetime*](https://python-driver.docs.scylladb.com/master/api/cassandra/query.md#cassandra.query.TraceEvent.datetime) *or* *timedelta* *or* *None*) – (optional) A datetime or timedelta object with desired timestamp to be applied
    to the batch conditional.
  * **consistency** (The [`ConsistencyLevel`](https://python-driver.docs.scylladb.com/master/api/cassandra.md#cassandra.ConsistencyLevel) to be used for the batch query, or None.) – (optional) One of consistency values (“ANY”, “ONE”, “QUORUM” etc)
  * **execute_on_exception** (*bool*) – (Defaults to False) Indicates that when the BatchQuery instance is used
    as a context manager the queries accumulated within the context must be executed despite
    encountering an error within the context. By default, any exception raised from within
    the context scope will cause the batched queries not to be executed.
  * **timeout** (*float* *or* *None*) – (optional) Timeout for the entire batch (in seconds), if not specified fallback
    to default session timeout
  * **connection** (*str*) – Connection name to use for the batch execution

#### add_query(query)

#### execute()

#### add_callback(fn, \*args, \*\*kwargs)

Add a function and arguments to be passed to it to be executed after the batch executes.

A batch can support multiple callbacks.

Note, that if the batch does not execute, the callbacks are not executed.
A callback, thus, is an “on batch success” handler.

* **Parameters:**
  * **fn** (*callable*) – Callable object
  * **args** – Positional arguments to be passed to the callback at the time of execution
  * **kwargs** – Named arguments to be passed to the callback at the time of execution

### *class* cassandra.cqlengine.query.ContextQuery(\*args, \*\*kwargs)

A Context manager to allow a Model to switch context easily. Presently, the context only
specifies a keyspace for model IO.

* **Parameters:**
  * **args** – One or more models. A model should be a class type, not an instance.
  * **kwargs** – (optional) Context parameters: can be *keyspace* or *connection*

For example:

```python
with ContextQuery(Automobile, keyspace='test2') as A:
    A.objects.create(manufacturer='honda', year=2008, model='civic')
    print(len(A.objects.all()))  # 1 result

with ContextQuery(Automobile, keyspace='test4') as A:
    print(len(A.objects.all()))  # 0 result

# Multiple models
with ContextQuery(Automobile, Automobile2, connection='cluster2') as (A, A2):
    print(len(A.objects.all()))
    print(len(A2.objects.all()))
```

### *class* cassandra.cqlengine.query.DoesNotExist

### *class* cassandra.cqlengine.query.MultipleObjectsReturned

### *class* cassandra.cqlengine.query.LWTException(existing)

Lightweight conditional exception.

This exception will be raised when a write using an IF clause could not be
applied due to existing data violating the condition. The existing data is
available through the `existing` attribute.

* **Parameters:**
  **existing** – The current state of the data which prevented the write.
