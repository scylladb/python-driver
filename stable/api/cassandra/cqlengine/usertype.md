# cassandra.cqlengine.usertype

Model classes for User Defined Types

<a id="module-cassandra.cqlengine.usertype"></a>

## UserType

### *class* cassandra.cqlengine.usertype.UserType(\*\*values)

This class is used to model User Defined Types. To define a type, declare a class inheriting from this,
and assign field types as class attributes:

```python
# connect with default keyspace ...

from cassandra.cqlengine.columns import Text, Integer
from cassandra.cqlengine.usertype import UserType

class address(UserType):
    street = Text()
    zipcode = Integer()

from cassandra.cqlengine import management
management.sync_type(address)
```

Please see [User Defined Types](https://python-driver.docs.scylladb.com/stable/cqlengine/models.md#user-types) for a complete example and discussion.

#### \_\_type_name_\_ *= None*

*Optional.* Sets the name of the CQL type for this type.

If not specified, the type name will be the name of the class, with it’s module name as it’s prefix.
