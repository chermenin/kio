---
title: Beam + Kotlin 1.4.30 = ☠️
layout: post
nav_show: false
author: Alex Chermenin
description: Small bug in the Kotlin compiler.
---

If you want to try using Beam with the latest version of Kotlin (currently `1.4.30`), you are likely to run into
the following (or very similar) problem while compiling your code:

```
Kotlin: [Internal Error] java.lang.IllegalStateException: Could not read class: VirtualFile: /Users/xx/.m2/repository/org/apache/beam/beam-sdks-java-core/2.27.0/beam-sdks-java-core-2.27.0.jar!/org/apache/beam/sdk/options/PipelineOptions.class
	at org.jetbrains.kotlin.load.java.structure.impl.classFiles.BinaryJavaClass.<init>(BinaryJavaClass.kt:120)
	at org.jetbrains.kotlin.load.java.structure.impl.classFiles.BinaryJavaClass.<init>(BinaryJavaClass.kt:34)
	at org.jetbrains.kotlin.cli.jvm.compiler.KotlinCliJavaFileManagerImpl.findClass(KotlinCliJavaFileManagerImpl.kt:115)
	...
Caused by: java.lang.IllegalArgumentException: Wildcard mast have a bound for annotation of WILDCARD_BOUND position
	at org.jetbrains.kotlin.load.java.structure.impl.classFiles.BinaryJavaAnnotation$Companion.computeTargetType$resolution_common_jvm(Annotations.kt:188)
	at org.jetbrains.kotlin.load.java.structure.impl.classFiles.AnnotationsAndParameterCollectorMethodVisitor.visitTypeAnnotation$getTargetType(Annotations.kt:111)
	...
```

The problem is independent of the Beam version and is directly related to the Kotlin compiler and the process of reading classes from files.
Here is the bug description in the issue tracker: [https://youtrack.jetbrains.com/issue/KT-45067](https://youtrack.jetbrains.com/issue/KT-45067).

The bug has now been fixed in the main branch of the Kotlin repository, but this fix will only be available to all
of us in version `1.5-M1`. Until then, there are no workarounds other than downgrading to version `1.4.21`.
