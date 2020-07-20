---
title: Joins
layout: default
nav_order: 4
parent: Developers guide
---

# Joins

Joins allow you to make queries over multiple bounded or unbounded collections at the same time.
Kio supports join operations for keyed collections and combine the data by keys over windows.
There is the following methods to combine data of several collections:

<h3>PCollection&lt;KV&lt;K,&nbsp;V&gt;&gt;</h3>

| Method | Description |
|:--|:--|
| **coGroup(_other_)**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;KV&lt;K,&nbsp;Pair&lt;Iterable&lt;V&gt;,&nbsp;Iterable&lt;X&gt;&gt;&gt;&gt;</code> | Groups elements of both input collections into the one by keys.<br /><br />_Arguments:_<br />·&nbsp;`other: PCollection<K, X>` - the second keyed collection to combine |
| **join(_other_)**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;KV&lt;K,&nbsp;Pair&lt;V,&nbsp;X&gt;&gt;&gt;</code> | Join elements of both input collections by keys and returns all pairs of the values.<br /><br />_Arguments:_<br />·&nbsp;`other: PCollection<K, X>` - the second keyed collection to join |
| **leftOuterJoin(_other_)**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;KV&lt;K,&nbsp;Pair&lt;V,&nbsp;X?&gt;&gt;&gt;</code> | Join elements of both input collections by keys and returns all pairs of the values and `null` value for keys not found in the second collection.<br /><br />_Arguments:_<br />·&nbsp;`other: PCollection<K, X>` - the second keyed collection to join |
| **rightOuterJoin(_other_)**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;KV&lt;K,&nbsp;Pair&lt;V?,&nbsp;X&gt;&gt;&gt;</code> | Join elements of both input collections by keys and returns all pairs of the values and `null` value for keys not found in the first collection.<br /><br />_Arguments:_<br />·&nbsp;`other: PCollection<K, X>` - the second keyed collection to join |
| **fullOuterJoin(_other_)**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;KV&lt;K,&nbsp;Pair&lt;V?,&nbsp;X?&gt;&gt;&gt;</code> | Join elements of both input collections by keys and returns all pairs of the values and `null` value for keys not found in any collection.<br /><br />_Arguments:_<br />·&nbsp;`other: PCollection<K, X>` - the second keyed collection to join |
| **subtractByKey(_other_)**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;KV&lt;K,&nbsp;V&gt;&gt;</code> | Returns a collection with all elements from the input collection with keys not found in the second collection.<br /><br />_Arguments:_<br />·&nbsp;`other: PCollection<K, X>` - the second keyed collection to combine |

<h3>PCollection&lt;T&gt;</h3>

| Method | Description |
|:--|:--|
| **intersection(_other_)**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;T&gt;</code> | Returns a collection of elements that exist in both input collections.<br /><br />_Arguments:_<br />·&nbsp;`other: PCollection<K, X>` - the second collection to combine |
| **subtract(_other_)**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;T&gt;</code> | Returns a collection of elements that are contained in the first input collection, but not in the second.<br /><br />_Arguments:_<br />·&nbsp;`other: PCollection<K, X>` - the second collection to combine |
