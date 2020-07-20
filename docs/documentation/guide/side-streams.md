---
title: Side streams
layout: default
nav_order: 3
parent: Developers guide
---

# Side streams
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Side inputs

In addition to the main collection, it's possible to provide other additional inputs to transformation as side inputs.
More information about side inputs you can find in the Beam's
[documentation](https://beam.apache.org/documentation/programming-guide/#side-inputs).

To use side inputs it's needed to create `PCollectionWithSideInput<T>` using the `withSideInputs` method of `PCollection<T>`.
This method receives one or multiple `PCollectionView` as the argument which represents the side inputs.

There is the following methods creating views from collections to use it as a side input:

<h3>PCollection&lt;T&gt;</h3>

| Method | Description |
|:--|:--|
| **asIterableView()**<br /><small>↪️</small>&nbsp;<code>PCollectionView&lt;Iterable&lt;T&gt;&gt;</code> | Takes the collection as input and produces a view mapping each window to an `Iterable` of the values in that window. |
| **asListView()**<br /><small>↪️</small>&nbsp;<code>PCollectionView&lt;List&lt;T&gt;&gt;</code> | Takes the input collection and returns a view mapping each window to a list containing all elements in the window. |
| **asSingletonView()**<br /><small>↪️</small>&nbsp;<code>PCollectionView&lt;T&gt;</code> | Takes the collection with a single value per window as input and produces a view that returns the value in the main input window when read as a side input. |

<h3>PCollection&lt;KV&lt;K,&nbsp;V&gt;&gt;</h3>

| Method | Description |
|:--|:--|
| **asMapView()**<br /><small>↪️</small>&nbsp;<code>PCollectionView&lt;Map&lt;K,&nbsp;V&gt;&gt;</code> | Takes the input collection and produces a view mapping each window to a map of key-value pairs in that window. |
| **asMultiMapView()**<br /><small>↪️</small>&nbsp;<code>PCollectionView&lt;Map&lt;K,&nbsp;Iterable&lt;V&gt;&gt;&gt;</code> | Takes the input collection and produces a view mapping each window to its contents as a `Map<K, Iterable<V>>`. |

After that you can use `map` and `flatMap` methods over created instance of `PCollectionWithSideInput<T>`:

| Method | Description |
|:--|:--|
| **flatMap(_f_)**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;U&gt;</code> | Takes each element with context, produces zero, one, or more elements, and returns a new `PCollection<U>` with all of them.<br /><br />_Arguments:_<br />·&nbsp;`f: (T, ProcessContext) -> Iterable<U>` - a function to transform the element (here you can use side inputs from the context) |
| **map(_f_)**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;U&gt;</code> | Takes each element with context, transforms it into a new one, and returns a new `PCollection<U>` with all of them.<br /><br />_Arguments:_<br />·&nbsp;`f: (T, ProcessContext) -> U` - a function to transform the element (here you can use side inputs from the context) |

## Side outputs

As well as creating side inputs for the transformation of the input collection you can produce any number of side outputs from it.
You should create an instance of `PCollectionWithSideOutput<T>` from the input collection using the `withSideOutputs` method
and define a set of tags for side outputs. After that you can also use `map` and `flatMap` methods:

| Method | Description |
|:--|:--|
| **flatMap(_f_)**<br /><small>↪️</small>&nbsp;<code>Pair&lt;PCollection&lt;U&gt;,&nbsp;PCollectionTuple&gt;</code> | Takes each element with context, produces zero, one, or more elements, and returns a new `PCollection<U>` with all of them.<br /><br />_Arguments:_<br />·&nbsp;`f: (T, ProcessContext) -> Iterable<U>` - a function to transform the element (here you can use the context to send results into a side output by the tag) |
| **map(_f_)**<br /><small>↪️</small>&nbsp;<code>Pair&lt;PCollection&lt;U&gt;,&nbsp;PCollectionTuple&gt;</code> | Takes each element with context, transforms it into a new one, and returns a new `PCollection<U>` with all of them.<br /><br />_Arguments:_<br />·&nbsp;`f: (T, ProcessContext) -> U` - a function to transform the element (here you can use the context to send results into a side output by the tag) |

These methods return a pair of the general output collection and the `PCollectionTuple` instance, from where you can get
side output collections by the tags. Also, there are built-in methods with side outputs:

| Method | Description |
|:--|:--|
| **splitBy(_p_)**<br /><small>↪️</small>&nbsp;<code>Pair&lt;PCollection&lt;T&gt;,&nbsp;PCollection&lt;T&gt;&gt;</code> | Takes the input collection and produces two: one with all elements matched to the predicate and another with the rest of the elements.<br /><br />_Arguments:_<br />·&nbsp;`p: (T) -> Boolean` - a function to check the element |
