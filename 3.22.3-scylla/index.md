# Python Driver for Scylla and Apache Cassandra®

A Python client driver for [Scylla](https://docs.scylladb.com).
This driver works exclusively with the Cassandra Query Language v3 (CQL3)
and Cassandra’s native protocol.

The driver supports Python 2.7, 3.4, 3.5, 3.6, 3.7 and 3.8.

This driver is open source under the
[Apache v2 License](http://www.apache.org/licenses/LICENSE-2.0.html).
The source code for this driver can be found on [GitHub](http://github.com/scylladb/python-driver).

Scylla Driver is a fork from [DataStax Python Driver](http://github.com/datastax/python-driver), including some non-breaking changes for Scylla optimization, with more updates planned.

## Contents

[Installation](installation.md)
: How to install the driver.

[Getting Started](getting_started.md)
: A guide through the first steps of connecting to Scylla and executing queries

[Scylla Specific Features](scylla_specific.md)
: A list of feature available only on `scylla-driver`

[Execution Profiles](execution_profiles.md)
: An introduction to a more flexible way of configuring request execution

[Lightweight Transactions (Compare-and-set)](lwt.md)
: Working with results of conditional requests

[Object Mapper](object_mapper.md)
: Introduction to the integrated object mapper, cqlengine

[Performance Notes](performance.md)
: Tips for getting good performance.

[Paging Large Queries](query_paging.md)
: Notes on paging large query results

[Security](security.md)
: An overview of the security features of the driver

[Upgrading](upgrading.md)
: A guide to upgrading versions of the driver

[User Defined Types](user_defined_types.md)
: Working with Scylla’s user-defined types (UDT)

[Working with Dates and Times](dates_and_times.md)
: Some discussion on the driver’s approach to working with timestamp, date, time types

[Scylla Cloud](scylla_cloud.md)
: Connect to Scylla Cloud

[CHANGELOG](CHANGELOG.md)
: Log of changes to the driver, organized by version.

[Frequently Asked Questions](faq.md)
: A collection of Frequently Asked Questions

[API Documentation](api/index.md)
: The API documentation.

## Getting Help

Visit the [FAQ section](faq.md) in this documentation.

Please send questions to the Scylla [user list](https://groups.google.com/forum/#!forum/scylladb-users).

## Reporting Issues

Please report any bugs and make any feature requests on the [Github project issues](https://github.com/scylladb/python-driver/issues)

## Copyright

© 2013-2017 DataStax

© 2016, The Apache Software Foundation.
Apache®, Apache Cassandra®, Cassandra®, the Apache feather logo and the Apache Cassandra® Eye logo are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries. No endorsement by The Apache Software Foundation is implied by the use of these marks.
