/*
 * Copyright 2020 Alex Chermenin
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

import java.io.Serializable
import kotlin.math.floor
import kotlin.math.sqrt
import org.apache.beam.sdk.coders.VarIntCoder
import org.apache.beam.sdk.values.KV
import org.junit.Assert.*
import org.junit.Test
import ru.chermenin.kio.coders.PairCoder
import ru.chermenin.kio.functions.*
import ru.chermenin.kio.test.*

class TransformFunctionsTest : KioPipelineTest() {

    @Test
    fun testSimpleMap() {
        val input = kio.parallelize(listOf(1, 2, 3))
        val results = input.map { (it * it).toString() }
        results.that().containsInAnyOrder("1", "4", "9")
        kio.execute()
    }

    @Test
    fun testMapToDataClass() {
        data class Event(val id: Int, val value: String) : Serializable

        val input = kio.parallelize(1..3)
        val results = input.map { Event(it, (it * it).toString()) }
        results.that().containsInAnyOrder(
            Event(1, "1"),
            Event(2, "4"),
            Event(3, "9")
        )
        kio.execute()
    }

    @Test
    fun testFlatMap() {
        val input = kio.parallelize(1..3)
        val results = input.flatMap { List(it) { index -> "*".repeat(index + 1) } }
        results.that().containsInAnyOrder("*", "*", "*", "**", "**", "***")
        kio.execute()
    }

    @Test
    fun testFlatMapValues() {
        val input = kio.parallelize(1..3).map { KV.of(it, it) }
        val results = input.flatMapValues { List(it) { index -> "*".repeat(index + 1) } }.toPair()
        results.that().containsInAnyOrder(
            1 to "*",
            2 to "*",
            2 to "**",
            3 to "*",
            3 to "**",
            3 to "***"
        )
        kio.execute()
    }

    @Test
    fun testDistinctFlatMap() {
        val input = kio.parallelize(1..3)
        val results = input.flatMap { List(it) { index -> "*".repeat(index + 1) } }.distinct()
        results.that().containsInAnyOrder("*", "**", "***")
        kio.execute()
    }

    @Test
    fun testFilter() {
        val input = kio.parallelize(0..10)
        val results = input.filter {
            val root = floor(sqrt(it.toDouble())).toInt()
            it == root * root
        }
        results.that().containsInAnyOrder(0, 1, 4, 9)
        kio.execute()
    }

    @Test
    fun testKeysAndValues() {
        val input = kio.parallelize(0..5)
        val results = input.keyBy { it % 2 }
        results.swap().toPair().that().containsInAnyOrder(
            0 to 0,
            1 to 1,
            2 to 0,
            3 to 1,
            4 to 0,
            5 to 1
        )
        results.keys().that().containsInAnyOrder(0, 1, 0, 1, 0, 1)
        results.values().that().containsInAnyOrder(0..5)
        kio.execute()
    }

    @Test
    fun testMapValues() {
        val input = kio.parallelize(listOf(1, 2, 3)).map { it to it }
            .setCoder(PairCoder.of(VarIntCoder.of(), VarIntCoder.of()))
            .toKV()
        val results = input.mapValues { (it * it).toString() }.toPair()
        results.that().containsInAnyOrder(
            1 to "1",
            2 to "4",
            3 to "9"
        )
        kio.execute()
    }

    @Test
    fun testPartitions() {
        val input = kio.parallelize(0..9)
        val partitions1 = input.partition(3)
        assertEquals(3, partitions1.size())
        val partitions2 = input.partitionBy(2) { it }
        assertEquals(2, partitions2.size())
        partitions2[0].that().containsInAnyOrder(0, 2, 4, 6, 8)
        partitions2[1].that().containsInAnyOrder(1, 3, 5, 7, 9)
        kio.execute()
    }

    @Test
    fun testSamples() {
        val input = kio.parallelize(0..9)
        input.sample(3).thatSingleton().satisfy { values -> assertEquals(3, values.count { it in 0..9 }) }
        input.take(4).that().satisfy { values -> assertEquals(4, values.count { it in 0..9 }) }
        kio.execute()
    }

    @Test
    fun testTop() {
        val input = kio.parallelize(0..9)
        input.top(4).thatSingleton().satisfy {
            assertEquals(4, it.size)
            assertTrue(it.containsAll(listOf(9, 8, 7, 6)))
        }
        kio.execute()
    }

    @Test
    fun testUnion() {
        val input1 = kio.parallelize(0..4)
        val input2 = kio.parallelize(5..9)
        val tuple = input1.and(input2)
        assertEquals(2, tuple.size())
        tuple[0].that().containsInAnyOrder(0..4)
        tuple[1].that().containsInAnyOrder(5..9)
        tuple.flatten().that().containsInAnyOrder(0..9)
        input1.union(input2).that().containsInAnyOrder(0..9)
        kio.execute()
    }
}
