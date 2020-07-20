---
title: SQL
layout: default
nav_order: 6
parent: Developers guide
---

# SQL
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Overview

This module allows executing SQL queries over bounded and unbounded collections. Because SQL statements can only be
applied to collections of rows, Kio provides some methods to transform Kotlin data classes to rows easier.

## Preparation methods

| Method | Description |
|:--|:--|
| **toRows()**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;Row&gt;</code> | Transforms the input collection of data class instances into the collection of rows. |
| **with(_collections_)**<br /><small>↪️</small>&nbsp;`PCollectionTuple` | Returns a `PCollectionTuple` with input collections mapped to the names.<br /><br />_Arguments:_<br />·&nbsp;`vararg collections: Pair<PCollection<Row>, String>` - a pair with an input collection of rows and a name for this collection |

## Query execution

There is the `sql` method for `PCollection<Row>` and `PCollectionTuple` which require a string with the SQL query as an argument.

In case when you use SQL queries over `PCollection<Row>` you should use `PCOLLECTION` as the name of the source in the `FROM` clause:

```kotlin
kio.parallelize(listOf(
        Person(0, "John", 26),
        Person(1, "Peter", 13),
        Person(2, "Tim", 64),
        Person(3, "Jane", 31),
        Person(4, "Bill", 17)
    ))
    .toRows()
    .sql("SELECT id FROM PCOLLECTION WHERE age >= 18")
```

To execute the SQL query over `PCollectionTuple` it's needed to use predefined names for sources:

```kotlin
val persons = kio.parallelize(listOf(
    Person(0, "John", 26, 2),
    Person(1, "Peter", 13, 0),
    Person(2, "Tim", 64, 1),
    Person(3, "Jane", 31, 1),
    Person(4, "Bill", 17, 2)
))

val cities = kio.parallelize(listOf(
    City(0, "Moscow"),
    City(1, "Paris"),
    City(2, "London"),
    City(4, "Tokyo")
))

with(
    persons.toRows() to "persons",
    cities.toRows() to "cities"
)
    .sql("""
        SELECT p.name
        FROM persons p
        JOIN cities c
          ON p.cityId = c.id
        WHERE c.name = 'Paris'
    """)
```
