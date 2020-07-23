---
title: Get started
layout: default
nav_order: 1
---

# Get Started
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Setup the environment

Before starting, you should check your development environment:

1. Download and install the [Java Development Kit (JDK)](https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html)
version 8. Verify that the `JAVA_HOME` environment variable points to your JDK installation.
2. Download and install [Apache Maven](https://maven.apache.org/download.cgi) by following Maven’s
[installation guide](https://maven.apache.org/install.html).

| We recommend using [IntelliJ IDEA](https://www.jetbrains.com/idea/download/) to just create a new project from the archetype (see the next section). |

## Create a new Kotlin project

Use Maven to create a new Kotlin project via the following command:

```sh
$ mvn archetype:generate \
      -DarchetypeGroupId=org.jetbrains.kotlin \
      -DarchetypeArtifactId=kotlin-archetype-jvm \
      -DarchetypeVersion=1.3.72 \
      -DgroupId=org.example \
      -DartifactId=kio-word-count \
      -Dversion="0.1" \
      -Dpackage=org.example.kio \
      -DinteractiveMode=false
```

This script will create a new directory `kio-word-count` with `pom.xml` and some other files.

## Configure the project

At the next step it's needed to add the following lines to the `pom.xml` file:

* Property for Kotlin compiler to set target JVM version (into the `properties` block):
  ```xml
  <kotlin.compiler.jvmTarget>1.8</kotlin.compiler.jvmTarget>
  ```

* Dependency description for Kio (into the `dependencies` block):
  ```xml
  <dependency>
      <groupId>ru.chermenin.kio</groupId>
      <artifactId>kio-core</artifactId>
      <version>x.y.z</version>
  </dependency>
  ```

## Create your first pipeline

1. Add a new file `WordCount.kt` to the `/src/main/kotlin/org/example/kio` directory.
1. Edit it to define a new `WordCount` object and the `main` method:
```kotlin
package org.example.kio
object WordCount {
  @JvmStatic
  fun main(args: Array<String>) {
  }
}
```
1. Create the Kio context:
```kotlin
val kio = Kio.fromArguments(args)
```
1. Read lines from the inputs:
```kotlin
val lines = kio.read().text(kio.arguments.required("input"))
```
1. Split line to words:
```kotlin
val words = lines.flatMap { it.split("\\W+".toRegex()) }
```
1. Filter empty values:
```kotlin
val filtered = words.filter { it.isNotBlank() }
```
1. Count words:
```kotlin
val counts = filtered.countByValue()
```
1. Format the results:
```kotlin
val results = counts.map { "${it.key}: ${it.value}" }
```
1. Write the results to the output:
```kotlin
results.write().text(kio.arguments.required("output"))
```
1. And execute the pipeline:
```kotlin
kio.execute().waitUntilFinish()
```

## Run the job

To execute the job run the command with `input` and `output` arguments:
```sh
$ mvn compile exec:java -Dexec.mainClass=org.example.kio.WordCount \
       -Dexec.args="--input=pom.xml --output=counts"
```

## Inspect the results

Once the pipeline has completed, you can view the output in multiple files prefixed by `counts`:
```sh
$ more counts*
executions: 2
count: 2
scope: 4
main: 1
dependencies: 2
plugins: 2
kotlin: 17
...
```

## What's next

Learn more about Kio viewing the [Developers Guide](guide)
and feel free to [help us]({{ '/documentation/' | absolute_url }}#contribute) with the project.
