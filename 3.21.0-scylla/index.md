# DataStax Python Driver for Apache Cassandra®

A Python client driver for [Apache Cassandra®](http://cassandra.apache.org).
This driver works exclusively with the Cassandra Query Language v3 (CQL3)
and Cassandra’s native protocol.  Cassandra 2.1+ is supported, including DSE 4.7+.

The driver supports Python 2.7, 3.4, 3.5, 3.6, 3.7 and 3.8.

This driver is open source under the
[Apache v2 License](http://www.apache.org/licenses/LICENSE-2.0.html).
The source code for this driver can be found on [GitHub](http://github.com/datastax/python-driver).

**Note:** DataStax products do not support big-endian systems.

## Contents

[Installation](installation.md)
: How to install the driver.

[Getting Started](getting_started.md)
: A guide through the first steps of connecting to Cassandra and executing queries

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
: Working with Cassandra 2.1’s user-defined types

[Working with Dates and Times](dates_and_times.md)
: Some discussion on the driver’s approach to working with timestamp, date, time types

[Cloud](cloud.md)
: A guide to connecting to Datastax Apollo

[DSE Geometry Types](geo_types.md)
: Working with DSE geometry types

[DataStax Graph Queries](graph.md)
: Graph queries with DSE Graph

[DataStax Graph Fluent API](graph_fluent.md)
: DataStax Graph Fluent API

[CHANGELOG](CHANGELOG.md)
: Log of changes to the driver, organized by version.

[Frequently Asked Questions](faq.md)
: A collection of Frequently Asked Questions

[API Documentation](api/index.md)
: The API documentation.

## Getting Help

Visit the [FAQ section](faq.md) in this documentation.

Please send questions to the [mailing list](https://groups.google.com/a/lists.datastax.com/forum/#!forum/python-driver-user).

Alternatively, you can use the [DataStax Community](https://community.datastax.com).

## Reporting Issues

Please report any bugs and make any feature requests on the
[JIRA](https://datastax-oss.atlassian.net/browse/PYTHON) issue tracker.

If you would like to contribute, please feel free to open a pull request.
