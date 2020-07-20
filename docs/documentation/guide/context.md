---
title: Context
layout: default
nav_order: 0
parent: Developers guide
---

# Kio Context
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Overview

Kio context is a main entry point for the application. It allows us to define and execute pipelines.
In general, the Kio context wraps Beam's `Pipeline` class in itself.

There are several ways to create the context:

1. Context with a default configuration:
```kotlin
val kio = Kio.create()
```
1. With the configuration from arguments:
```kotlin
val kio = Kio.fromArguments(args)
```
1. From an existing `Pipeline` instance:
```kotlin
val kio = Kio.fromPipeline(pipeline)
```

## Arguments

In case when you create the context from arguments, you have access to additional ones
(not related to the defined runner) using the `arguments` property of the context.
There are the following methods to work with these additional arguments:

| Method&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; | Description |
|:--|:--|
| `required(key): String`<br />`operator get(key): String` | Get a value for required argument as string.<br />If there are no arguments with the defined key, it will throw the `IllegalArgumentException` exception.<br /><br />_Arguments:_<br />·&nbsp;`key: String` - the key of the argument to get the value |
| `optinal(key): String?` | Get a value for defined argument as string or null if there are no arguments with the defined key.<br /><br />_Arguments:_<br />·&nbsp;`key: String` - the key of the argument to get the value |
| `int(key): Int`<br />`long(key): Long`<br />`float(key): Float`<br />`double(key): Double`<br />`bool(key): Boolean` | Methods to get a typed value for the argument with the defined key. If the value cannot be cast to the defined type, it will throw the `IllegalArgumentException` exception.<br /><br />_Arguments:_<br />·&nbsp;`key: String` - the key of the argument to get the value |
| `int(key, default): Int`<br />`long(key, default): Long`<br />`float(key, default): Float`<br />`double(key, default): Double`<br />`bool(key, default): Boolean` | Methods to get a typed value for the argument with the defined key. If there are no arguments with the defined key, the default value will be used. If the value cannot be cast to the defined type, it will throw the `IllegalArgumentException` exception.<br /><br />_Arguments:_<br />·&nbsp;`key: String` - the key of the argument to get the value<br />·&nbsp;`default: () -> T` - lambda to get the default value |

## Creating a pipeline

A pipeline can be created using readers (see [IO basics](io.html) and [Connectors](../connectors)).

Also, there is the `parallelize` method to transform any collection to the pipeline:
```kotlin
val collection: PCollection<Int> = kio.parallelize(listOf(1, 2, 3, 4, 5))
```

This method can receive any implementations of the `Iterable` interface as the argument value.

## Running the pipeline

There is the `execute` method to run the defined pipeline:
```kotlin
kio.execute()
```

It returns an `ExecutionContext` instance with the following additional method:

| Method | Description |
|:--|:--|
| `getState(): State` | Getting current [state](https://beam.apache.org/releases/javadoc/2.20.0/org/apache/beam/sdk/PipelineResult.State.html) of executed pipeline. |
| `isCompleted(): Boolean` | Returns true if the pipeline is in any terminal state, and false otherwise. |
| `waitUntilFinish(duration)` | Makes the application wait until the pipeline is finished.<br /><br />_Arguments:_<br />·&nbsp;`duration: Duration` (optional) - time period after which the job will be interrupted |
| `waitUntilDone(duration)` | Makes the application wait until the pipeline is done.<br />If the task is completed with a state other than `DONE`, it will throw an exception.<br /><br />_Arguments:_<br />·&nbsp;`duration: Duration` (optional) - time period after which the job will be interrupted |
