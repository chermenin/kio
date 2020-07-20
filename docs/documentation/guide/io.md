---
title: IO basics
layout: default
nav_order: 1
parent: Developers guide
---

# Input and output basics
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Readers

A reader is a general element to get data from any sources. The Kio context class has the `read` method to create
an abstract reader instance. The reader instance has methods getting data, and the set of these methods depends
on the set of attached connectors via the dependencies list (see [Connectors](../connectors)).

The `core` module contains built-in methods to read data from text files: 

```kotlin
kio.read().text(
    "/path/to/input.txt",
    compression = Compression.GZIP
)
```

This method returns a `PCollection` of `Strings`, each corresponding to one line of the input text file.
The `compression` argument is optional and has `Compression.AUTO` as the default value. All other possible
values you can find [here](https://beam.apache.org/releases/javadoc/2.20.0/org/apache/beam/sdk/io/Compression.html).

## Writers

Writers are responsible for sending results into various sinks. The set of the sink methods also depends
on the set of attached connectors via the dependencies list (see [Connectors](../connectors)).

All instances of the `PCollection` class in Kio have the `write` method to create an abstract writer instance,
and the `core` module contains built-in methods to write data to text files:

```kotlin
collection.write().text(
    path = "/path/to/output",
    numShards = 5,
    suffix = ".txt",
    compression = Compression.GZIP
)
```

The `path` argument is required, all other arguments are optional:
* `numShards: Int` - number of output files (default: `0` - by deciding of the runner)
* `suffix: String` - name ending for the output files (default: `.txt`)
* `compression: Compression` - method to compress output files (default: `Compression.UNCOMPRESSED`)
