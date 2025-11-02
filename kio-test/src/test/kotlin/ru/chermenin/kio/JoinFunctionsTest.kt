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

import org.apache.beam.sdk.values.KV
import org.junit.Test
import ru.chermenin.kio.functions.*
import ru.chermenin.kio.test.KioPipelineTest
import ru.chermenin.kio.test.that

class JoinFunctionsTest : KioPipelineTest() {

    @Test
    fun testIntersection() {
        val input1 = kio.parallelize(0..19)
        val input2 = kio.parallelize(14..32)
        val results = input1.intersection(input2)
        results.that().containsInAnyOrder(14..19)
        kio.execute()
    }

    @Test
    fun testSubtract() {
        val input1 = kio.parallelize(0..19)
        val input2 = kio.parallelize(14..32)
        val results = input1.subtract(input2)
        results.that().containsInAnyOrder(0..13)
        kio.execute()
    }

    @Test
    fun testSubtractByKey() {
        val input1 = kio.parallelize(0..7).keyBy { it % 3 }
        val input2 = kio.parallelize(14..17).keyBy { it % 2 }
        val results = input1.subtractByKey(input2)
        results.that().containsInAnyOrder(
            KV.of(2, 2),
            KV.of(2, 5)
        )
        kio.execute()
    }

    @Test
    fun testJoin() {
        val input1 = kio.parallelize(0..7).keyBy { it % 3 }
        val input2 = kio.parallelize(14..17).keyBy { it % 2 }
        val results = input1.join(input2)
        results.that().containsInAnyOrder(
            KV.of(0, Pair(0, 14)),
            KV.of(0, Pair(0, 16)),
            KV.of(0, Pair(3, 14)),
            KV.of(0, Pair(3, 16)),
            KV.of(0, Pair(6, 14)),
            KV.of(0, Pair(6, 16)),
            KV.of(1, Pair(1, 15)),
            KV.of(1, Pair(1, 17)),
            KV.of(1, Pair(7, 15)),
            KV.of(1, Pair(7, 17)),
            KV.of(1, Pair(4, 15)),
            KV.of(1, Pair(4, 17))
        )
        kio.execute()
    }

    @Test
    fun testLeftOuterJoin() {
        val input1 = kio.parallelize(0..7).keyBy { it % 3 }
        val input2 = kio.parallelize(14..17).keyBy { it % 2 }
        val results = input1.leftOuterJoin(input2)
        results.that().containsInAnyOrder(
            KV.of(0, Pair(0, 14)),
            KV.of(0, Pair(0, 16)),
            KV.of(0, Pair(3, 14)),
            KV.of(0, Pair(3, 16)),
            KV.of(0, Pair(6, 14)),
            KV.of(0, Pair(6, 16)),
            KV.of(1, Pair(1, 15)),
            KV.of(1, Pair(1, 17)),
            KV.of(1, Pair(7, 15)),
            KV.of(1, Pair(7, 17)),
            KV.of(1, Pair(4, 15)),
            KV.of(1, Pair(4, 17)),
            KV.of(2, Pair(2, null)),
            KV.of(2, Pair(5, null))
        )
        kio.execute()
    }

    @Test
    fun testRightOuterJoin() {
        val input1 = kio.parallelize(0..7).keyBy { it % 3 }
        val input2 = kio.parallelize(14..17).keyBy { it % 4 }
        val results = input1.rightOuterJoin(input2)
        results.that().containsInAnyOrder(
            KV.of(0, Pair(0, 16)),
            KV.of(0, Pair(3, 16)),
            KV.of(0, Pair(6, 16)),
            KV.of(1, Pair(1, 17)),
            KV.of(1, Pair(4, 17)),
            KV.of(1, Pair(7, 17)),
            KV.of(2, Pair(2, 14)),
            KV.of(2, Pair(5, 14)),
            KV.of(3, Pair(null, 15))
        )
        kio.execute()
    }

    @Test
    fun testFullOuterJoin() {
        val input1 = kio.parallelize(-2..7).keyBy { it % 3 }
        val input2 = kio.parallelize(14..17).keyBy { it % 4 }
        val results = input1.fullOuterJoin(input2)
        results.that().containsInAnyOrder(
            KV.of(-2, Pair(-2, null)),
            KV.of(-1, Pair(-1, null)),
            KV.of(0, Pair(0, 16)),
            KV.of(0, Pair(3, 16)),
            KV.of(0, Pair(6, 16)),
            KV.of(1, Pair(1, 17)),
            KV.of(1, Pair(4, 17)),
            KV.of(1, Pair(7, 17)),
            KV.of(2, Pair(2, 14)),
            KV.of(2, Pair(5, 14)),
            KV.of(3, Pair(null, 15))
        )
        kio.execute()
    }
}
