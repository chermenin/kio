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
import kotlin.math.abs
import kotlin.math.roundToInt
import kotlin.math.sin
import org.apache.beam.sdk.coders.ListCoder
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.values.KV
import org.joda.time.Instant
import org.junit.Assert.*
import org.junit.Test
import ru.chermenin.kio.functions.*
import ru.chermenin.kio.test.KioPipelineTest
import ru.chermenin.kio.test.satisfy
import ru.chermenin.kio.test.that
import ru.chermenin.kio.test.thatSingleton

class CombineFunctionsTest : KioPipelineTest() {

    companion object {
        private const val delta = 0.00000001
    }

    @Test
    fun testCombine() {
        val input = kio.parallelize(0..10)
        val results = input.combine(
            { listOf(it.toString()) },
            { acc: List<String>, elem: Int -> acc + elem.toString() },
            { acc1, acc2 -> acc1 + acc2 },
            coder = ListCoder.of(StringUtf8Coder.of())
        )
        results.thatSingleton().satisfy { value -> assertTrue(value.containsAll((0..10).map { it.toString() })) }
        kio.execute()
    }

    @Test
    fun testCombineByKey() {
        val input = kio.parallelize(0..10).keyBy { it % 2 }
        val results = input.combineByKey(
            { listOf(it.toString()) },
            { acc: List<String>, elem: Int -> acc + elem.toString() },
            { acc1, acc2 -> acc1 + acc2 },
            coder = ListCoder.of(StringUtf8Coder.of())
        )
        results.that().satisfy { iterable ->
            iterable.forEach {
                if (it.key == 0) {
                    val values0 = listOf("0", "2", "4", "6", "8", "10")
                    assertTrue(it.value.subtract(values0).isEmpty())
                    assertTrue(values0.subtract(it.value).isEmpty())
                } else {
                    val values1 = listOf("1", "3", "5", "7", "9")
                    assertTrue(it.value.subtract(values1).isEmpty())
                    assertTrue(values1.subtract(it.value).isEmpty())
                }
            }
        }
        kio.execute()
    }

    @Test
    fun testAggregate() {
        val input = kio.parallelize(0..10)
        val results = input.aggregate(
            "--",
            { acc: String, elem: Int -> acc + elem.toString() },
            { acc1, acc2 -> acc1 + acc2 }
        )
        results.thatSingleton().satisfy { value ->
            assertTrue(value.toList().containsAll((0..10).joinToString(prefix = "--", separator = "").toList()))
        }
        kio.execute()
    }

    @Test
    fun testAggregateByKey() {
        val input = kio.parallelize(0..10).keyBy { it % 2 }
        val results = input.aggregateByKey(
            "--",
            { acc: String, elem: Int -> acc + elem.toString() },
            { acc1, acc2 -> acc1 + acc2 }
        )
        results.that().satisfy { values ->
            values.forEach { value ->
                if (value.key == 0) {
                    assertTrue(value.value.toList()
                        .containsAll(listOf(0, 2, 4, 6, 8, 10).joinToString(prefix = "--", separator = "").toList()))
                } else {
                    assertTrue(value.value.toList()
                        .containsAll(listOf(1, 3, 5, 7, 9).joinToString(prefix = "--", separator = "").toList()))
                }
            }
        }
        kio.execute()
    }

    @Test
    fun testFold() {
        val input = kio.parallelize(1..5)
        val results = input.fold(-1) { a, b -> a * b }
        results.thatSingleton().satisfy { assertEquals(-120, it) }
        kio.execute()
    }

    @Test
    fun testFoldByKey() {
        val input = kio.parallelize(1..10).keyBy { it % 2 }
        val results = input.foldByKey(-1) { a, b -> a * b }
        results.that().satisfy { iterable ->
            iterable.forEach {
                if (it.key == 0) {
                    assertEquals(-3840, it.value)
                } else {
                    assertEquals(-945, it.value)
                }
            }
        }
        kio.execute()
    }

    @Test
    fun testReduce() {
        val input = kio.parallelize(1..5)
        val results = input.reduce { a, b -> a + b }
        results.thatSingleton().satisfy { assertEquals(15, it) }
        kio.execute()
    }

    @Test
    fun testReduceByKey() {
        val input = kio.parallelize(1..10).keyBy { it % 2 }
        val results = input.reduceByKey { a, b -> a + b }
        results.that().satisfy { iterable ->
            iterable.forEach {
                if (it.key == 0) {
                    assertEquals(30, it.value)
                } else {
                    assertEquals(25, it.value)
                }
            }
        }
        kio.execute()
    }

    @Test
    fun testAapproximateQuantiles() {
        val input = kio.parallelize(0..99)
        val results = input.approximateQuantiles(6)
        results.thatSingleton().satisfy {
            assertEquals(6, it.size)
            assertTrue(it.containsAll(listOf(0, 19, 39, 59, 79, 99)))
        }
        kio.execute()
    }

    @Test
    fun testAapproximateQuantilesByKey() {
        val input = kio.parallelize(0..99).keyBy { it % 3 }
        val results = input.approximateQuantilesByKey(7)
        results.that().satisfy { iterable ->
            iterable.forEach {
                when (it.key) {
                    0 -> {
                        assertEquals(7, it.value.size)
                        assertTrue(it.value.containsAll(listOf(0, 15, 33, 48, 66, 84, 99)))
                    }
                    1 -> {
                        assertEquals(7, it.value.size)
                        assertTrue(it.value.containsAll(listOf(1, 16, 31, 49, 64, 82, 97)))
                    }
                    2 -> {
                        assertEquals(7, it.value.size)
                        assertTrue(it.value.containsAll(listOf(2, 17, 32, 50, 65, 83, 98)))
                    }
                }
            }
        }
        kio.execute()
    }

    @Test
    fun testCountApproxDistinctWithEstimationError() {
        val input = kio.parallelize(0..99).map { 1.0 * it / 9.0 }
        val results = input.countApproxDistinct(0.1)
        results.thatSingleton().isEqualTo(100)
        kio.execute()
    }

    @Test
    fun testCountApproxDistinctByKeyWithEstimationError() {
        val input = kio.parallelize(0..99).keyBy { it % 3 }.mapValues { 1.0 * it / 17.0 }
        val results = input.countApproxDistinctByKey(0.2)
        results.that().satisfy { iterable ->
            iterable.forEach {
                when (it.key) {
                    0 -> assertEquals(34, it.value)
                    1 -> assertEquals(33, it.value)
                    2 -> assertEquals(33, it.value)
                }
            }
        }
        kio.execute()
    }

    @Test
    fun testCountApproxDistinctWithSampleSize() {
        val input = kio.parallelize(0..99).map { 1.0 * it / 9.0 }
        val results = input.countApproxDistinct(40)
        results.thatSingleton().isEqualTo(125)
        kio.execute()
    }

    @Test
    fun testCountApproxDistinctByKeyWithSampleSize() {
        val input = kio.parallelize(0..99).keyBy { it % 3 }.mapValues { 1.0 * it / 17.0 }
        val results = input.countApproxDistinctByKey(20)
        results.that().satisfy { iterable ->
            iterable.forEach {
                when (it.key) {
                    0 -> assertEquals(43, it.value)
                    1 -> assertEquals(27, it.value)
                    2 -> assertEquals(29, it.value)
                }
            }
        }
        kio.execute()
    }

    @Test
    fun testCount() {
        val input = kio.parallelize(0..99)
        val results = input.count()
        results.thatSingleton().isEqualTo(100)
        kio.execute()
    }

    @Test
    fun testCountByKey() {
        val input = kio.parallelize(0..99).keyBy { it % 7 }
        val results = input.countByKey()
        results.that().satisfy { iterable ->
            iterable.forEach {
                when (it.key) {
                    0 -> assertEquals(15, it.value)
                    1 -> assertEquals(15, it.value)
                    2 -> assertEquals(14, it.value)
                    3 -> assertEquals(14, it.value)
                    4 -> assertEquals(14, it.value)
                    5 -> assertEquals(14, it.value)
                    6 -> assertEquals(14, it.value)
                }
            }
        }
        kio.execute()
    }

    @Test
    fun testCountByValue() {
        val input = kio.parallelize(0..99).map { (10.0 * sin(it.toDouble())).roundToInt() % 7 }
        val results = input.countByValue()
        results.that().satisfy { iterable ->
            iterable.forEach {
                when (it.key) {
                    -6 -> assertEquals(4, it.value)
                    -5 -> assertEquals(3, it.value)
                    -4 -> assertEquals(4, it.value)
                    -3 -> assertEquals(15, it.value)
                    -2 -> assertEquals(9, it.value)
                    -1 -> assertEquals(9, it.value)
                    0 -> assertEquals(14, it.value)
                    1 -> assertEquals(9, it.value)
                    2 -> assertEquals(8, it.value)
                    3 -> assertEquals(15, it.value)
                    4 -> assertEquals(5, it.value)
                    5 -> assertEquals(2, it.value)
                    6 -> assertEquals(3, it.value)
                }
            }
            assertEquals(100, iterable.map { it.value }.reduce { a, b -> a + b })
        }
        kio.execute()
    }

    @Test
    fun testGroupByKey() {
        val input = kio.parallelize(0..99).keyBy { it / 10 }
        val results = input.groupByKey()
        results.that().satisfy { iterable ->
            iterable.forEach {
                val resultList = it.value.toList()
                assertEquals(10, resultList.size)
                val checkRange = (it.key!! * 10)..(it.key!! * 10 + 9)
                assertTrue(resultList.containsAll(checkRange.toList()))
            }
        }
        kio.execute()
    }

    @Test
    fun testGroupBy() {
        data class Key(val id: Int) : Serializable

        val input = kio.parallelize(0..99)
        val results = input.groupBy { Key(it / 10) }
        results.that().satisfy { iterable ->
            iterable.forEach {
                val resultList = it.value.toList()
                assertEquals(10, resultList.size)
                val checkRange = (it.key!!.id * 10)..(it.key!!.id * 10 + 9)
                assertTrue(resultList.containsAll(checkRange.toList()))
            }
        }
        kio.execute()
    }

    @Test
    fun testGroupToBatches() {
        val input = kio.parallelize(0..99).keyBy { it / 10 }
        val results = input.groupToBatches(4)
        results.that().satisfy { iterable ->
            iterable.forEach {
                assertTrue(it.value.count() <= 4)
            }
        }
        kio.execute()
    }

    @Test
    fun testLatest() {
        val input = kio.parallelize("hello world".toCharArray().asIterable())
        val results = input.withTimestamps { Instant.ofEpochSecond(it.toLong()) }.latest()
        results.thatSingleton().isEqualTo('w')
        kio.execute()
    }

    @Test
    fun testLatestByKey() {
        val input = kio.parallelize("hello world".toCharArray().asIterable())
            .withTimestamps { Instant.ofEpochSecond(it.toLong()) }
            .keyBy { it.toInt() % 3 }
        val results = input.latestByKey()
        results.that().satisfy { iterable ->
            iterable.forEach {
                when (it.key) {
                    0 -> assertEquals('r', it.value)
                    1 -> assertEquals('d', it.value)
                    2 -> assertEquals('w', it.value)
                }
            }
        }
        kio.execute()
    }

    @Test
    fun testMinMax() {
        val input = kio.parallelize("Hello, World!".toCharArray().asIterable())
        input.max().thatSingleton().isEqualTo('r')
        input.min().thatSingleton().isEqualTo(' ')
        kio.execute()
    }

    @Test
    fun testMinMaxByKey() {
        val input = kio.parallelize("Hello, World!".toCharArray().asIterable()).keyBy { it.toInt() % 2 }
        input.maxByKey().that().satisfy { iterable ->
            iterable.forEach {
                when (it.key) {
                    0 -> assertEquals('r', it.value)
                    1 -> assertEquals('o', it.value)
                }
            }
        }
        input.minByKey().that().satisfy { iterable ->
            iterable.forEach {
                when (it.key) {
                    0 -> assertEquals(' ', it.value)
                    1 -> assertEquals('!', it.value)
                }
            }
        }
        kio.execute()
    }

    @Test
    fun testMean() {
        val ints = kio.parallelize(-3..10)
        ints.mean().thatSingleton().satisfy {
            assertEquals(3.5, it, delta)
        }
        val longs = kio.parallelize(3L..12L)
        longs.mean().thatSingleton().satisfy {
            assertEquals(7.5, it, delta)
        }
        val floats = kio.parallelize(listOf(0.1f, 0.2f, 0.3f, 0.4f))
        floats.mean().thatSingleton().satisfy {
            assertEquals(0.25, it, delta)
        }
        val doubles = kio.parallelize(listOf(1.0 / 2.0, 1.0 / 3.0, 1.0 / 4.0))
        doubles.mean().thatSingleton().satisfy {
            assertEquals(0.361111111, it, delta)
        }
        kio.execute()
    }

    @Test
    fun testMeanByKey() {
        val ints = kio.parallelize(-11..10).keyBy { abs(it % 2) }
        ints.meanByKey().that().satisfy { iterable ->
            iterable.forEach {
                when (it.key) {
                    0 -> assertEquals(0.0, it.value, delta)
                    1 -> assertEquals(-1.0, it.value, delta)
                }
            }
        }
        val longs = kio.parallelize(3L..12L).keyBy { it % 2 }
        longs.meanByKey().that().satisfy { iterable ->
            iterable.forEach {
                when (it.key) {
                    0L -> assertEquals(8.0, it.value, delta)
                    1L -> assertEquals(7.0, it.value, delta)
                }
            }
        }
        val floats = kio.parallelize(listOf(0.1f, 0.2f, 0.3f, 0.4f)).keyBy { (it * 10).roundToInt() % 2 }
        floats.meanByKey().that().satisfy { iterable ->
            iterable.forEach {
                when (it.key) {
                    0 -> assertEquals(0.3, it.value, delta)
                    1 -> assertEquals(0.2, it.value, delta)
                }
            }
        }
        val doubles = kio.parallelize(listOf(
            KV.of("a", 1.0 / 2.0),
            KV.of("b", 1.0 / 3.0),
            KV.of("a", 1.0 / 4.0),
            KV.of("b", 1.0 / 5.0)
        ))
        doubles.meanByKey().that().satisfy { iterable ->
            iterable.forEach {
                when (it.key) {
                    "a" -> assertEquals(0.375, it.value, delta)
                    "b" -> assertEquals(0.266666666, it.value, delta)
                }
            }
        }
        kio.execute()
    }

    @Test
    fun testSum() {
        val ints = kio.parallelize(-10..10)
        ints.sum().thatSingleton().isEqualTo(0)
        val longs = kio.parallelize(3L..12L)
        longs.sum().thatSingleton().isEqualTo(75L)
        val floats = kio.parallelize(listOf(0.1f, 0.2f, 0.3f, 0.4f))
        floats.sum().thatSingleton().satisfy {
            assertEquals(1.0f, it, delta.toFloat())
        }
        val doubles = kio.parallelize(listOf(1.0 / 2.0, 1.0 / 3.0, 1.0 / 4.0))
        doubles.sum().thatSingleton().satisfy {
            assertEquals(1.083333333, it, delta)
        }
        kio.execute()
    }

    @Test
    fun testSumByKey() {
        val ints = kio.parallelize(-11..10).keyBy { abs(it % 2) }
        ints.sumByKey().that().satisfy { iterable ->
            iterable.forEach {
                when (it.key) {
                    0 -> assertEquals(0, it.value)
                    1 -> assertEquals(-11, it.value)
                }
            }
        }
        val longs = kio.parallelize(3L..12L).keyBy { it % 2 }
        longs.sumByKey().that().satisfy { iterable ->
            iterable.forEach {
                when (it.key) {
                    0L -> assertEquals(40, it.value)
                    1L -> assertEquals(35, it.value)
                }
            }
        }
        val floats = kio.parallelize(listOf(0.1f, 0.2f, 0.3f, 0.4f)).keyBy { (it * 10).roundToInt() % 2 }
        floats.sumByKey().that().satisfy { iterable ->
            iterable.forEach {
                when (it.key) {
                    0 -> assertEquals(0.6f, it.value, delta.toFloat())
                    1 -> assertEquals(0.4f, it.value, delta.toFloat())
                }
            }
        }
        val doubles = kio.parallelize(listOf(
            KV.of("a", 1.0 / 2.0),
            KV.of("b", 1.0 / 3.0),
            KV.of("a", 1.0 / 4.0),
            KV.of("b", 1.0 / 5.0)
        ))
        doubles.sumByKey().that().satisfy { iterable ->
            iterable.forEach {
                when (it.key) {
                    "a" -> assertEquals(0.75, it.value, delta)
                    "b" -> assertEquals(0.533333333, it.value, delta)
                }
            }
        }
        kio.execute()
    }

    @Test
    fun testTopByKey() {
        val input = kio.parallelize(1..100).keyBy { abs(it % 3) }
        val results = input.topByKey(3)
        results.that().satisfy { iterable ->
            iterable.forEach {
                when (it.key) {
                    0 -> {
                        assertEquals(3, it.value.size)
                        assertTrue(it.value.containsAll(listOf(93, 96, 99)))
                    }
                    1 -> {
                        assertEquals(3, it.value.size)
                        assertTrue(it.value.containsAll(listOf(94, 97, 100)))
                    }
                    2 -> {
                        assertEquals(3, it.value.size)
                        assertTrue(it.value.containsAll(listOf(92, 95, 98)))
                    }
                }
            }
        }
        kio.execute()
    }
}
