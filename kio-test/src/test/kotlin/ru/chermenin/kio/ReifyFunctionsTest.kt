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

import org.apache.beam.sdk.coders.KvCoder
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.coders.VarIntCoder
import org.apache.beam.sdk.coders.VarLongCoder
import org.apache.beam.sdk.values.KV
import org.joda.time.Duration
import org.joda.time.Instant
import org.junit.Test
import ru.chermenin.kio.coders.PairCoder
import ru.chermenin.kio.functions.*
import ru.chermenin.kio.test.KioPipelineTest
import ru.chermenin.kio.test.that

class ReifyFunctionsTest : KioPipelineTest() {

    @Test
    fun testAsTimestamped() {
        val input = kio.parallelize(listOf("a", "b", "c", "d", "e", "f"))
            .withTimestamps { Instant.ofEpochMilli((it[0] - 'a').toLong()) }
        val results = input.asTimestamped()
            .map { it.value to it.timestamp.millis }
            .setCoder(PairCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))
        results.that().containsInAnyOrder(
            "a" to 0L,
            "b" to 1L,
            "c" to 2L,
            "d" to 3L,
            "e" to 4L,
            "f" to 5L
        )
        kio.execute()
    }

    @Test
    fun testAsTimestampedValues() {
        val input = kio.parallelize(listOf("a", "b", "c", "d", "e", "f"))
            .withTimestamps { Instant.ofEpochMilli((it[0] - 'a').toLong()) }
            .keyBy { (it[0] - 'a') % 2 }
        val results = input.asTimestampedValues()
            .mapValues { it.value to it.timestamp.millis }
            .setCoder(KvCoder.of(VarIntCoder.of(), PairCoder.of(StringUtf8Coder.of(), VarLongCoder.of())))
        results.that().containsInAnyOrder(
            KV.of(0, "a" to 0L),
            KV.of(1, "b" to 1L),
            KV.of(0, "c" to 2L),
            KV.of(1, "d" to 3L),
            KV.of(0, "e" to 4L),
            KV.of(1, "f" to 5L)
        )
        kio.execute()
    }

    @Test
    fun testAsWindowed() {
        val input = kio.parallelize(listOf("a", "b", "c", "d", "e", "f"))
            .withTimestamps { Instant.ofEpochMilli((it[0] - 'a').toLong()) }
            .withFixedWindow(Duration.millis(3))
        val results = input.asWindowed()
            .map { it.value to it.window.maxTimestamp().millis }
            .setCoder(PairCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))
        results.that().containsInAnyOrder(
            "a" to 2L,
            "b" to 2L,
            "c" to 2L,
            "d" to 5L,
            "e" to 5L,
            "f" to 5L
        )
        kio.execute()
    }

    @Test
    fun testAsWindowedValues() {
        val input = kio.parallelize(
            listOf(
                "a" to 0,
                "b" to 5,
                "c" to 10,
                "d" to 40,
                "e" to 55,
                "f" to 60
            ),
            coder = PairCoder.of(StringUtf8Coder.of(), VarIntCoder.of())
        )
            .withTimestamps { Instant.ofEpochMilli(it.second.toLong()) }
            .map { it.first }
            .keyBy { (it[0] - 'a') % 2 }
            .withSessionWindow(Duration.millis(15))
        val results = input.asWindowedValues()
            .mapValues { it.value to it.window.maxTimestamp().millis }
            .setCoder(KvCoder.of(VarIntCoder.of(), PairCoder.of(StringUtf8Coder.of(), VarLongCoder.of())))
        results.that().containsInAnyOrder(
            KV.of(0, "a" to 14L),
            KV.of(1, "b" to 19L),
            KV.of(0, "c" to 24L),
            KV.of(1, "d" to 54L),
            KV.of(0, "e" to 69L),
            KV.of(1, "f" to 74L)
        )
        kio.execute()
    }
}
