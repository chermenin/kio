---
title: Testing
layout: default
nav_order: 5
---

# Testing
{: .no_toc }

In general, testing for Kio based on the same approach as for Beam, so before starting take a look at Beam's testing
[documentation](https://beam.apache.org/documentation/pipelines/test-your-pipeline/).

To create a test for your pipeline it's needed to add the `kio-test` module as a dependency and use
`ru.chermenin.kio.test.KioPipelineTest` as the base class. This class has already defined context as the `kio` property.

Also, there is the following methods to get [assertion instances](https://beam.apache.org/documentation/pipelines/test-your-pipeline/#passert)
from any collection:

### PCollection&lt;T&gt;

| Method | Description |
|:--|:--|
| **that(_reason_)**<br /><small>↪️</small>&nbsp;<code>PAssert.IterableAssert&lt;T&gt;</code> | Returns an iterable assert for the elements of the provided collection.<br /><br />_Arguments:_<br />·&nbsp;`reason` - optional reason for the assertion |
| **thatSingleton(_reason_)**<br /><small>↪️</small>&nbsp;<code>PAssert.SingletonAssert&lt;T&gt;</code> | Returns a singleton assert for the value of the provided collection, which must be a singleton.<br /><br />_Arguments:_<br />·&nbsp;`reason` - optional reason for the assertion |

### PCollection&lt;Iterable&lt;T&gt;&gt;

| Method | Description |
|:--|:--|
| **thatSingletonIterable(_reason_)**<br /><small>↪️</small>&nbsp;<code>PAssert.IterableAssert&lt;T&gt;</code> | Returns an iterable assert for the value of the provided collection, which must contain a single iterable value.<br /><br />_Arguments:_<br />·&nbsp;`reason` - optional reason for the assertion |

### PCollection&lt;KV&lt;K,&nbsp;V&gt;&gt;

| Method | Description |
|:--|:--|
| **thatMap(_reason_)**<br /><small>↪️</small>&nbsp;<code>PAssert.SingletonAssert&lt;Map&lt;K,&nbsp;V&gt;&gt;</code> | Returns a singleton assert for the value of the provided collection, which must have at most one value per key.<br /><br />_Arguments:_<br />·&nbsp;`reason` - optional reason for the assertion |
| **thatMultiMap(_reason_)**<br /><small>↪️</small>&nbsp;<code>PAssert.SingletonAssert&lt;Map&lt;K,&nbsp;Iterable&lt;V&gt;&gt;&gt;</code> | Returns a singleton assert for the value of the provided collection.<br /><br />_Arguments:_<br />·&nbsp;`reason` - optional reason for the assertion |
