/*
 * Copyright 2020-2025 Alex Chermenin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ru.chermenin.kio

import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.coders.VarIntCoder
import org.joda.time.Duration
import org.joda.time.Instant
import org.junit.Assert.assertEquals
import org.junit.Test
import ru.chermenin.kio.coders.PairCoder
import ru.chermenin.kio.functions.*
import ru.chermenin.kio.test.KioPipelineTest
import ru.chermenin.kio.test.satisfy
import ru.chermenin.kio.test.that

class WindowFunctionsTest : KioPipelineTest() {

    @Test
    fun testGlobalWindow() {
        val input = kio.parallelize(listOf("a", "b", "c", "d", "e", "f"))
            .withTimestamps { Instant.ofEpochMilli((it[0] - 'a').toLong()) }
        val results = input
            .withFixedWindow(Duration.millis(3))
            .withGlobalWindow()
            .top(10)
        results.that().satisfy { iterable ->
            val resultsList = iterable.toList().map { it.sorted() }.sortedBy { it[0] }
            assertEquals(listOf(listOf("a", "b", "c", "d", "e", "f")), resultsList)
        }
        kio.execute()
    }

    @Test
    fun testFixedWindow() {
        val input = kio.parallelize(listOf("a", "b", "c", "d", "e", "f"))
            .withTimestamps { Instant.ofEpochMilli((it[0] - 'a').toLong()) }
        val results = input.withFixedWindow(Duration.millis(3)).top(10)
        results.that().satisfy { iterable ->
            val resultsList = iterable.toList().map { it.sorted() }.sortedBy { it[0] }
            assertEquals(
                listOf(
                    listOf("a", "b", "c"),
                    listOf("d", "e", "f")
                ),
                resultsList
            )
        }
        kio.execute()
    }

    @Test
    fun testSlidingWindow() {
        val input = kio.parallelize(listOf("a", "b", "c", "d", "e", "f"))
            .withTimestamps { Instant.ofEpochMilli((it[0] - 'a').toLong()) }
        val results = input.withSlidingWindow(Duration.millis(3), Duration.millis(2)).top(10)
        results.that().satisfy { iterable ->
            val resultsList = iterable.toList().map { it.sorted() }.sortedBy { it[0] + it.size }
            assertEquals(
                listOf(
                    listOf("a"),
                    listOf("a", "b", "c"),
                    listOf("c", "d", "e"),
                    listOf("e", "f")
                ),
                resultsList
            )
        }
        kio.execute()
    }

    @Test
    fun testSessionWindow() {
        val input = kio.parallelize(listOf(
            "a" to 0,
            "b" to 5,
            "c" to 10,
            "d" to 40,
            "e" to 55,
            "f" to 60
        ), coder = PairCoder.of(StringUtf8Coder.of(), VarIntCoder.of()))
            .withTimestamps { Instant.ofEpochMilli(it.second.toLong()) }
            .map { it.first }
        val results = input.withSessionWindow(Duration.millis(10)).top(10)
        results.that().satisfy { iterable ->
            val resultsList = iterable.toList().map { it.sorted() }.sortedBy { it[0] }
            assertEquals(
                listOf(
                    listOf("a", "b", "c"),
                    listOf("d"),
                    listOf("e", "f")
                ),
                resultsList
            )
        }
        kio.execute()
    }
}
