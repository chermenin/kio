---
title: Connectors
layout: default
nav_order: 3
has_children: true
---

# Connectors

Below you can find the list of connectors implemented in Kio. Also, it's possible to use all IO connectors from Beam
as usual: [https://beam.apache.org/documentation/io/built-in/](https://beam.apache.org/documentation/io/built-in/). 

Most of the connectors are configurable and have the `with` method for that. Here is an example:

```kotlin
val collection = kio.read()
    .kafka<Int, String>()
    .with { it.withBootstrapServers("kafka1:9091") }
    .topic("input_topic")
```
