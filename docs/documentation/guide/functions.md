---
title: Functions
layout: default
nav_order: 2
parent: Developers guide
---

# Functions
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Transformations

Transformation functions allow modifying elements of collections in various ways.
In this section you can find functions related to each type of collections.

### PCollection&lt;T&gt;

| Method | Description |
|:--|:--|
| **distinct()**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;T&gt;</code> | Returns a collection with all distinct elements by the window. |
| **filter(_f_)**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;T&gt;</code> | Returns a `PCollection<T>` with elements matched to the predicate.<br /><br />_Arguments:_<br />·&nbsp;`f: (T) -> Boolean` - the predicate functions to check the elements |
| **flatMap(_f_)**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;U&gt;</code> | Takes each element, produces zero, one, or more elements, and returns a new `PCollection<U>` with all of them.<br /><br />_Arguments:_<br />·&nbsp;`f: (T) -> Iterable<U>` - a function to transform the element |
| **forEach(_f_)**<br /><small>↪️</small>&nbsp;`Unit` | Applies the defined functions to each element from the input and returns nothing.<br /><br />_Arguments:_<br />·&nbsp;`f: (T) -> Unit` - a function to apply |
| **keyBy(_f_)**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;KV&lt;K,&nbsp;T&gt;&gt;</code> | Assigns a new key to each value and returns a collection of key-value pairs.<br /><br />_Arguments:_<br />·&nbsp;`f: (T) -> K` - a function to extract the key from the element |
| **map(_f_)**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;U&gt;</code> | Takes each element from the input, transforms it into a new one, and returns a new `PCollection<U>` with all of them.<br /><br />_Arguments:_<br />·&nbsp;`f: (T) -> U` - a function to transform the element |
| **partition(_partitions_)**<br /><small>↪️</small>&nbsp;<code>PCollectionList&lt;T&gt;</code> | Splits the elements of the input collection into the defined number of partitions, and returns a `PCollectionList<T>` that bundles all collections containing the split elements.<br /><br />_Arguments:_<br />·&nbsp;`partitions: Int` - a number of partitions |
| **partitionBy(_partitions_,&nbsp;_f_)**<br /><small>↪️</small>&nbsp;<code>PCollectionList&lt;T&gt;</code> | Uses the defined function to split the elements of the input collection into partitions, and returns a `PCollectionList<T>` that bundles all collections containing the split elements.<br /><br />_Arguments:_<br />·&nbsp;`partitions: Int` - a number of partitions<br />·&nbsp;`f: (T) -> Int` - a function to define the number of partition for the element |
| **peek(_f_)**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;T&gt;</code> | Applies the defined functions to each element and returns the same collection.<br /><br />_Arguments:_<br />·&nbsp;`f: (T) -> Unit` - a function to apply |
| **sample(_sampleSize_)**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;Iterable&lt;T&gt;&gt;</code> | Uniformly at random selects the defined number of the elements from the input and returns a collection containing the selected elements.<br /><br />_Arguments:_<br />·&nbsp;`sampleSize: Int` - a size of the sample |
| **take(_limit_)**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;T&gt;</code> | Returns a new collection containing up to limit elements of the input.<br /><br />_Arguments:_<br />·&nbsp;`limit: Long` - a size of the limit |
| **top(_count_)**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;List&lt;T&gt;&gt;</code> | Returns a new collection with a single element containing the largest elements of the input collection.<br /><br />_Arguments:_<br />·&nbsp;`count: Int` - a limit count of the results |

### PCollection&lt;KV&lt;K,&nbsp;T&gt;&gt;

| Method | Description |
|:--|:--|
| **flatMapValues(_f_)**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;KV&lt;K,&nbsp;U&gt;&gt;</code> | Takes the value from each input key-value pair, produces zero, one, or more elements, and returns a new collection with key-value pairs for all results.<br /><br />_Arguments:_<br />·&nbsp;`f: (V) -> Iterable<U>` - a function to transform the value |
| **keys()**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;K&gt;</code> | Returns a collection of the keys from the input collection of key-value pairs. |
| **mapValues(_f_)**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;KV&lt;K,&nbsp;U&gt;&gt;</code> | Takes the value from each input key-value pair, transforms it into a new one, and returns a new collection with key-value pairs with the results as values.<br /><br />_Arguments:_<br />·&nbsp;`f: (V) -> U` - a function to transform the value |
| **swap()**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;KV&lt;V,&nbsp;K&gt;&gt;</code> | Returns a new collection, where all the keys and values ​​have been swapped. |
| **toPair()**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;Pair&lt;K,&nbsp;V&gt;&gt;</code> | Returns a collection of Kotlin `Pair` instances from the input key-value pairs. |
| **values()**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;V&gt;</code> | Returns a collection of the values from the input collection of key-value pairs. |

### PCollection&lt;Pair&lt;K,&nbsp;T&gt;&gt;

| Method | Description |
|:--|:--|
| **toKV()**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;KV&lt;K,&nbsp;V&gt;&gt;</code> | Returns a new collection of `KV` instances from the input collection of Kotlin `Pair` elements. |

## Combinations

Combining functions are responsible for aggregating elements of the input collections.

### PCollection&lt;T&gt;

| Method | Description |
|:--|:--|
| **and(_pc_)**<br /><small>↪️</small>&nbsp;<code>PCollectionList&lt;T&gt;</code> | Combines input collections to a list.<br /><br />_Arguments:_<br />·&nbsp;`pc: PCollection<T>` - other collection to combine |
| **approximateQuantiles(_num_)**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;List&lt;T&gt;&gt;</code> | Returns a collection with a single value as a list of the approximate N-tiles of the elements of the input collection.<br /><br />_Arguments:_<br />·&nbsp;`num: Int` - a size of quantiles list |
| **aggregate(_zero_,&nbsp;_seq_,&nbsp;_comb_)**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;U&gt;</code> | Combines elements from the input collection using given functions and the beginning value.<br /><br />_Arguments:_<br />·&nbsp;`zero: U` - a beginning value to aggregate<br />·&nbsp;`seq: (U, T) -> U` - adds the current element to result<br />·&nbsp;`comb: (U, U) -> U` - combines two results into the one |
| **combine(_cc_,&nbsp;_mv_,&nbsp;_mc_)**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;U&gt;</code> | Combines elements from the input collection using given functions.<br /><br />_Arguments:_<br />·&nbsp;`cc: (T) -> U` - creates a new combiner from the element<br />·&nbsp;`mv: (U, T) -> U` - adds the current element to the combiner<br />·&nbsp;`mc: (U, U) -> U` - merges two combiners into the one |
| **countApproxDistinct(_maxErr_)**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;Long&gt;</code> | Returns a collection with a single value that is an estimate of the number of distinct elements in the input collection.<br /><br />_Arguments:_<br />·&nbsp;`maxErr: Double` - a desired maximum estimation error  |
| **countApproxDistinct(_sampleSize_)**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;Long&gt;</code> | Returns a collection with a single value that is an estimate of the number of distinct elements in the input collection.<br /><br />_Arguments:_<br />·&nbsp;`sampleSize: Int` - controls the estimation error, which is about `2 / sqrt(sampleSize)` |
| **count()**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;Long&gt;</code> | Returns a collection with a single value that is the number of elements in the input collection. |
| **countByValue()**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;KV&lt;T,&nbsp;Long&gt;&gt;</code> | Returns a keyed collection with the elements from the input collection as the keys and counts of them as the values. |
| **fold(_zero_,&nbsp;_f_)**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;T&gt;</code> | Folds all elements from the input collection to the one result using the given function and the beginning value.<br /><br />_Arguments:_<br />·&nbsp;`zero: T` - a beginning value to aggregate<br />·&nbsp;`f: (T, T) -> T` - combines two elements into the one result |
| **groupBy(_f_)**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;KV&lt;K,&nbsp;Iterable&lt;T&gt;&gt;&gt;</code> | Returns a keyed collection representing a map from each distinct key from the elements of the input collection to an `Iterable` over all the values associated with that key.<br /><br />_Arguments:_<br />·&nbsp;`f: (T) -> K` - a function to extract a key from the element |
| **latest()**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;T&gt;</code> | Returns a collection with the latest element according to its event time, or null if there are no elements. |
| **max()**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;T&gt;</code> | Returns a collection with the maximum according to the natural ordering of `T` of the input elements, or null if there are no elements. |
| **min()**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;T&gt;</code> | Returns a collection with the minimum according to the natural ordering of `T` of the input elements, or null if there are no elements. |
| **reduce(_f_)**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;T&gt;</code> | Reduces all elements from the input collection to the one result using the given function.<br /><br />_Arguments:_<br />·&nbsp;`f: (T, T) -> T` - combines two elements into the one result |
| **union(_pc_)**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;T&gt;</code> | Returns a new collection with all elements from the input collections.<br /><br />_Arguments:_<br />·&nbsp;`pc: PCollection<T>` - other collection to combine |

### PCollection&lt;T&nbsp;:&nbsp;Number&gt;

| Method | Description |
|:--|:--|
| **mean()**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;Double&gt;</code> | Returns a collection with the mean of the input elements, or `0` if there are no elements. |
| **sum()**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;T&gt;</code> | Returns a collection with the sum of the input elements, or `0` if there are no elements. |

### PCollection&lt;KV&lt;K,&nbsp;T&gt;&gt;

| Method | Description |
|:--|:--|
| **aggregateByKey(_zero_,&nbsp;_seq_,&nbsp;_comb_)**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;KV&lt;K,&nbsp;U&gt;&gt;</code> | Combines values from the input key-value pairs using given functions and the beginning value.<br /><br />_Arguments:_<br />·&nbsp;`zero: U` - a beginning value to aggregate<br />·&nbsp;`seq: (U, V) -> U` - adds the current value to result<br />·&nbsp;`comb: (U, U) -> U` - combines two results into the one |
| **approximateQuantilesByKey(_num_)**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;KV&lt;K,&nbsp;List&lt;V&gt;&gt;&gt;</code> | Returns a collection with lists of the approximate N-tiles of the values for each key of the input key-value pairs.<br /><br />_Arguments:_<br />·&nbsp;`num: Int` - a size of quantiles list |
| **combineByKey(_cc_,&nbsp;_mv_,&nbsp;_mc_)**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;KV&lt;K,&nbsp;U&gt;&gt;</code> | Combines values from the input key-value pairs using given functions.<br /><br />_Arguments:_<br />·&nbsp;`cc: (V) -> U` - creates a new combiner from the value<br />·&nbsp;`mv: (U, V) -> U` - adds the current value to the combiner<br />·&nbsp;`mc: (U, U) -> U` - merges two combiners into the one |
| **countByKey()**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;KV&lt;K,&nbsp;Long&gt;&gt;</code> | Returns a collection with counts of values for each key in the input key-value pairs. |
| **foldByKey(_zero_,&nbsp;_f_)**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;KV&lt;K,&nbsp;V&gt;&gt;</code> | Folds all values for each key from the input key-value pairs to the one result using the given function and the beginning value.<br /><br />_Arguments:_<br />·&nbsp;`zero: V` - a beginning value to aggregate<br />·&nbsp;`f: (V, V) -> V` - combines two values into the one result |
| **groupByKey()**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;KV&lt;K,&nbsp;Iterable&lt;V&gt;&gt;&gt;</code> | Returns a keyed collection representing a map from each distinct key from the elements of the input collection to an `Iterable` over all the values associated with that key. |
| **groupToBatches(_size_)**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;KV&lt;K,&nbsp;Iterable&lt;V&gt;&gt;&gt;</code> | Combines the input key-value pairs to a desired batch size for each key. |
| **latestByKey()**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;KV&lt;K,&nbsp;V&gt;&gt;</code> | Returns a collection with the latest values for each key according to its event time. |
| **maxByKey()**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;KV&lt;K,&nbsp;V&gt;&gt;</code> | Returns a collection with the maximum value for each key according to the natural ordering of `V`. |
| **minByKey()**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;KV&lt;K,&nbsp;V&gt;&gt;</code> | Returns a collection with the minimum value for each key according to the natural ordering of `V`. |
| **reduceByKey(_f_)**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;KV&lt;K,&nbsp;V&gt;&gt;</code> | Reduces all values for each key from the input key-value pairs to the one result using the given function.<br /><br />_Arguments:_<br />·&nbsp;`f: (V, V) -> V` - combines two values into the one result |

### PCollection&lt;KV&lt;K,&nbsp;V&nbsp;:&nbsp;Number&gt;&gt;

| Method | Description |
|:--|:--|
| **meanByKey()**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;KV&lt;K,&nbsp;Double&gt;&gt;</code> | Returns a collection with the means of the input values for each key. |
| **sumByKey()**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;KV&lt;K,&nbsp;V&gt;&gt;</code> | Returns a collection with the sums of the input values for each key. |

## Reifying

Reifying functions for converting between the explicit and implicit forms of various Beam values.

### PCollection&lt;T&gt;

| Method | Description |
|:--|:--|
| **asTimestamped()**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;TimestampedValue&lt;T&gt;&gt;</code> | Returns a collection with all elements of the input wrapped in values with timestamps. |
| **asWindowed()**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;ValueInSingleWindow&lt;T&gt;&gt;</code> | Returns a collection with all elements of the input enriched with additional information about windowing. |

### PCollection&lt;KV&lt;K,&nbsp;V&gt;&gt;

| Method | Description |
|:--|:--|
| **asTimestampedValues()**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;KV&lt;K,&nbsp;TimestampedValue&lt;T&gt;&gt;&gt;</code> | Returns a collection with all values of the input key-value pairs wrapped in values with timestamps. |
| **asWindowedValues()**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;KV&lt;K,&nbsp;ValueInSingleWindow&lt;T&gt;&gt;&gt;</code> | Returns a collection with all values of the input key-value pairs enriched with additional information about windowing. |
