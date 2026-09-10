# `cassandra.cqlengine.columns` - Column types for object mapping models

## Columns

Columns in your models map to columns in your CQL table. You define CQL columns by defining column attributes on your model classes.
For a model to be valid it needs at least one primary key column and one non-primary key column.

Just as in CQL, the order you define your columns in is important, and is the same order they are defined in on a model’s corresponding table.

Each column on your model definitions needs to be an instance of a Column class.

## Column Types

Columns of all types are initialized by passing `Column` attributes to the constructor by keyword.
