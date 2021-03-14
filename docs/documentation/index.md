---
title: Overview
layout: default
nav_order: 0
---

# Overview
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## What is Kio?

**Kio** is a set of Kotlin extensions for [Apache Beam](https://beam.apache.org) implementing fluent-like API for Java SDK.

In general, Kio just adds some methods to the classes from Apache Beam. So, if you are a novice in distributed data
processing, please check out the Beam's [programming guide](https://beam.apache.org/documentation/programming-guide/)
first for a detailed explanation of the Beam programming model and concepts.

Thanks to the wonderful interact between Kotlin and Java you can use all methods from Kio and usual Beam Java SDK in
the same program, but it can lead to poor readability. Therefore, we recommend you keep one style where possible.

## Modules

The Kio project contains the following main modules:
* **core** - common classes and methods (see [Developers Guide](guide))
* **connectors** - modules to connect with various third-party systems (see [Connectors](connectors))
* **test** - a module to make testing of pipelines easier (see [Testing](testing.html))
* **cep** - a module with methods for complex event processing (see [Complex Event Processing](cep.html))

## Motivation

The main reason we founded the project is that the Beam Java SDK is very different to APIs of other tools like Spark,
Flink, Scalding, Pandas, etc. The reasons for the Beam community chose this approach are well described in
[this blog post](https://beam.apache.org/blog/where-is-my-pcollection-dot-map/).

Kotlin allowed extending the API without losing full compatibility. Now you don't need to look through the documentation
to find `PTransform` class name what you should use in one place or another. Your IDE does everything for you:

| With Kio | Without Kio |
|--|--|
| ![]({{ '/assets/images/documentation/with_kio.png' | absolute_url }}) | ![]({{ '/assets/images/documentation/without_kio.png' | absolute_url }}) |

Also, this project is a good place for experiments like the [CEP](cep.html) module.
