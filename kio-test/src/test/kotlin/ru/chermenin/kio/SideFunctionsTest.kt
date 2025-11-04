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

import kotlin.math.sqrt
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.values.TupleTag
import org.junit.Assert.assertEquals
import org.junit.Test
import ru.chermenin.kio.functions.*
import ru.chermenin.kio.test.KioPipelineTest
import ru.chermenin.kio.test.satisfy
import ru.chermenin.kio.test.that

class SideFunctionsTest : KioPipelineTest() {

    companion object {
        private const val delta = 0.00000001
    }

    @Test
    fun testIterableViewAsSideInput() {
        val input = kio.parallelize(1..4)
        val sideInput = kio.parallelize('a'..'d').asIterableView()
        val results = input.withSideInputs(sideInput).flatMap<Int, String> { element, context ->
            val sideElements = context.sideInput(sideInput)
            sideElements.map { "$it$element" }
        }
        results.that().containsInAnyOrder(
            "a1", "a2", "a3", "a4",
            "b1", "b2", "b3", "b4",
            "c1", "c2", "c3", "c4",
            "d1", "d2", "d3", "d4"
        )
        kio.execute()
    }

    @Test
    fun testListViewAsSideInput() {
        val input = kio.parallelize(1..4)
        val sideInput = kio.parallelize('a'..'d').asListView()
        val results = input.withSideInputs(sideInput).flatMap<Int, String> { element, context ->
            val sideElements = context.sideInput(sideInput)
            sideElements.map { "$it$element" }
        }
        results.that().containsInAnyOrder(
            "a1", "a2", "a3", "a4",
            "b1", "b2", "b3", "b4",
            "c1", "c2", "c3", "c4",
            "d1", "d2", "d3", "d4"
        )
        kio.execute()
    }

    @Test
    fun testSingletonViewAsSideInput() {
        val input = kio.parallelize(1..4)
        val sideInput = kio.parallelize(listOf('a')).asSingletonView()
        val results = input.withSideInputs(sideInput).map<Int, String> { element, context ->
            val sideElements = context.sideInput(sideInput)
            "$sideElements$element"
        }
        results.that().containsInAnyOrder(
            "a1",
            "a2",
            "a3",
            "a4"
        )
        kio.execute()
    }

    @Test
    fun testSideOutput() {
        val input = kio.parallelize(0..4)
        val sideOutputTag = TupleTag<String>()
        val (results, sideOutputs) = input.withSideOutputs(sideOutputTag).map<Int, Double> { element, context ->
            context.output(sideOutputTag, "" + ('a' + element))
            sqrt(element.toDouble())
        }
        results.that().satisfy {
            val values = it.toList().sorted()
            assertEquals(values[0], 0.0, delta)
            assertEquals(values[1], 1.0, delta)
            assertEquals(values[2], 1.41421356237, delta)
            assertEquals(values[3], 1.73205080756, delta)
            assertEquals(values[4], 2.0, delta)
        }
        sideOutputs[sideOutputTag]
            .setCoder(StringUtf8Coder.of())
            .that().containsInAnyOrder("a", "b", "c", "d", "e")
        kio.execute()
    }

    @Test
    fun testSplit() {
        val input = kio.parallelize(0..9)
        val (even, odd) = input.splitBy { it % 2 == 0 }
        even.that().containsInAnyOrder(0, 2, 4, 6, 8)
        odd.that().containsInAnyOrder(1, 3, 5, 7, 9)
        kio.execute()
    }
}
