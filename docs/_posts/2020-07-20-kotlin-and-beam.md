---
title: Kotlin + Apache Beam = ❤️
layout: post
nav_show: false
author: Alex Chermenin
description: In a nutshell, why Kotlin is the best language for your Apache Beam pipelines.
---

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Why Kotlin?

[Kotlin](https://kotlinlang.org/) is a general-purpose, free, open-source, statically typed "pragmatic"
programming language initially designed for the JVM that combines object-oriented and functional
programming features. From the beginning, it is focused on interoperability, safety, and clarity.

There is a post about Kotlin in Apache Beam's blog: [https://beam.apache.org/blog/beam-kotlin/](https://beam.apache.org/blog/beam-kotlin/).
But we decided to go further and implement extensions to make creating data pipelines much easier.

In general, Kotlin looks like a more concise and optimized version of Java, so it allows us to write
code for data pipelines faster and more clearly. And in this post, I'll show you how our life changes
when we replace Java with Kotlin using one pipeline as an example.

## WordCount example

So, let's take a look at the word count job written using Beam's Java SDK:

```java
PipelineOptions options = PipelineOptionsFactory.create();

Pipeline p = Pipeline.create(options);

p.apply(TextIO.read().from("input"))
    .apply(
        FlatMapElements.into(TypeDescriptors.strings())
            .via((String line) -> Arrays.asList(line.split("\\W+")))
    )
    .apply(Filter.by((String word) -> !word.isEmpty()))
    .apply(Count.perElement())
    .apply(
        MapElements.into(TypeDescriptors.strings())
            .via(
                (KV<String, Long> wordCount) ->
                    wordCount.getKey() + ": " + wordCount.getValue()
            )
    )
    .apply(TextIO.write().to("output"));

p.run().waitUntilFinish();
```

As you can see, the most used method of the pipeline is `apply`, which receives an instance of any
transformation as the argument. And no matter what IDE you use, it won't help you choose the
transformation to use next in your pipeline. Because of this, you are forced to view through
the documentation every time.

But what if to use Kotlin extension methods from Kio while creating the word count job?

```kotlin
val kio = Kio.fromArguments(args)

kio.read().text("input")
    .flatMap { it.split("\\W+".toRegex()) }
    .filter { it.isNotBlank() }
    .countByValue()
    .map { "${it.key}: ${it.value}" }
    .write().text("output")

kio.execute().waitUntilFinish()
```

I'm sure this code looks more familiar to everyone who at least once used such tools as Spark, Flink, Pandas, etc.
It looks significantly more readable than the previous one, and by the way, you will be able to implement
your jobs more rapidly because your IDE will help you with this.

## Few words about plans

At the moment, our main goal is to cover the code with tests better. In parallel with this, we would like
to add some new features for experimental Beam's methods to work with schemas. Of course, it's needed to implement
all necessary connectors. So, in one word, there is still a lot of work before we can release version 1.0.
