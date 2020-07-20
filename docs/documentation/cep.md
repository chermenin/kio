---
title: CEP
layout: default
nav_order: 4
---

# Complex Event Processing
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Overview

**Complex event processing** (CEP) is an approach to predict high-level events likely to result from specific sets of
low-level factors. CEP identifies and analyzes cause-and-effect relationships among events in real-time, allowing
proactively take effective actions in response to specific scenarios.

Speaking of Kio, the CEP module allows you to describe a pattern as a sequence of events and conditions for them and
apply this pattern to the input collection, receiving a collection of complex events collected according to the
defined pattern as the output.

## Pattern definitions

There are the following methods to define a pattern:

| Method | Description |
|:--|:--|
| **startWith(_name_,&nbsp;_condition_)** | Static method of the `Pattern` class to defines a beginning of a pattern.<br /><br />_Arguments:_<br />·&nbsp;`name: String` - the name of the pattern element<br />·&nbsp;`condition: (T) -> Boolean` - a condition to check the element |
| **then(_name_,&nbsp;_condition_)** | Appends a new pattern element with the defined name, which matches an event from the input collection that should directly succeed to the previous matched event (strict contiguity).<br /><br />_Arguments:_<br />·&nbsp;`name: String` - the name of the pattern element<br />·&nbsp;`condition: (T) -> Boolean` - a condition to check the element |
| **thenFollowBy(_name_,&nbsp;_condition_)** | Appends a new pattern element with the defined name. Other events not matched to the condition can occur between a matched event and the previous matched event (relaxed contiguity).<br /><br />_Arguments:_<br />·&nbsp;`name: String` - the name of the pattern element<br />·&nbsp;`condition: (T) -> Boolean` - a condition to check the element |
| **thenFollowByAny(_name_,&nbsp;_condition_)** | Appends a new pattern element with the defined name. Other events can occur between a matched event and the previous matched event, and all alternative complex events will be presented for every alternative matched event (non-deterministic relaxed contiguity).<br /><br />_Arguments:_<br />·&nbsp;`name: String` - the name of the pattern element<br />·&nbsp;`condition: (T) -> Boolean` - a condition to check the element |

Here is an example of a pattern definition and applying:

```kotlin
// pattern definition
val pattern: Pattern<Event> = Pattern
    .startWith<Event>("start") { it.name == "start" }
    .then("middle") { it.name == "middle" && it.value > 5.5 }
    .thenFollowByAny("end") { it.name == "end" }
    .within(Duration.standardSeconds(10))

// applying to the input
val complexCollection: PCollection<ComplexEvent<Event>> =
    input.match(pattern, allowedLateness = Duration.standardSeconds(30))

// mapping the complex events
complexCollection.map {
    val start = it["start"].elementAt(0)
    val middle = it["middle"].elementAt(0)
    val end = it["end"].elementAt(0)
    "${start.id} -> ${middle.id} -> ${end.id}"
}
```

### Timed patterns

Timed patterns have the maximum time interval for an event sequence to match. If a non-completed event sequence exceeds
this time, it is discarded. To define the timed pattern, it's needed to use `within(duration)` method.

### Windowed patterns

Windowed patterns don't have any limitations by time but all events must be in the same window to be matched. There is
the `withinWindow()` method for that.

## Pattern matching

There are the following methods to apply the defined pattern to the input collection:

| Method | Description |
|:--|:--|
| `PCollection<T>`<br />**.match(_pattern_,&nbsp;_allowedLateness_)**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;ComplexEvent&lt;T&gt;&gt;</code> | Applies the defined pattern to the input collection and returns a new collection of complex events.<br /><br />_Arguments:_<br />·&nbsp;`pattern: Pattern<T>` - a pattern to apply<br />·&nbsp;`allowedLateness: Duration` - optional value determining the duration of a possible lag of events |
| <code>PCollection<KV<K,&nbsp;V>></code><br />**.matchValues(_pattern_,&nbsp;_allowedLateness_)**<br /><small>↪️</small>&nbsp;<code>PCollection&lt;ComplexEvent&lt;T&gt;&gt;</code> | Applies the defined pattern to the values of the input collection and returns a new collection of complex events for each key.<br /><br />_Arguments:_<br />·&nbsp;`pattern: Pattern<V>` - a pattern to apply<br />·&nbsp;`allowedLateness: Duration` - optional value determining the duration of a possible lag of events |


## CEP for Java SDK

There is the `CEP` object witch has the following methods to allow using pattern matching via pure Beam's Java SDK:

| **matchValues(_pattern_)**<br /><small>↪️</small>&nbsp;<code>PTransform&lt;PCollection&lt;KV&lt;K,&nbsp;V&gt;&gt;,&nbsp;PCollection&lt;KV&lt;K,&nbsp;ComplexEvent&lt;T&gt;&gt;&gt;&gt;</code><br /><br />Returns a transformation to apply the defined pattern to values from the input collection and transform it into a new collection of complex events.<br /><br />_Arguments:_<br />·&nbsp;`pattern: Pattern<V>` - a pattern to apply |
| **lateMatchValues(_pattern_,&nbsp;_allowedLateness_,&nbsp;_keyClass_,&nbsp;_valueClass_)**<br /><small>↪️</small>&nbsp;<code>PTransform&lt;PCollection&lt;KV&lt;K,&nbsp;V&gt;&gt;,&nbsp;PCollection&lt;KV&lt;K,&nbsp;ComplexEvent&lt;T&gt;&gt;&gt;&gt;</code><br /><br />Returns a transformation to apply the defined pattern to values from the input collection with a specified duration waiting for late events and transform it into a new collection of complex events.<br /><br />_Arguments:_<br />·&nbsp;`pattern: Pattern<T>` - a pattern to apply<br />·&nbsp;`allowedLateness: Duration` - a value determining the duration of a possible lag of events<br />·&nbsp;`keyClass: Class<K>` - the class of keys in the input collection<br />·&nbsp;`valueClass: Class<V>` - the class of values in the input collection |
