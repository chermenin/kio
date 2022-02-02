/*
 * Copyright 2021 Alex Chermenin
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

import org.joda.time.Duration
import org.junit.Assert.*
import org.junit.Test
import ru.chermenin.kio.functions.*
import ru.chermenin.kio.test.*

class GeneratorsTest : KioPipelineTest() {

    @Test
    fun testSimpleGenerator() {
        val results = kio.generate(from = 0, to = 10)
        results.that().containsInAnyOrder(0L..9L)
        kio.execute()
    }

    @Test
    fun testRatedGenerator() {
        val input = kio.generate(
            from = 0,
            to = 100,
            rate = 10L to Duration.standardSeconds(1)
        )
        val results = input
            .withFixedWindow(Duration.standardSeconds(2))
            .count()
        results.that().satisfy { resultList ->
            val restList = resultList.filter { it != 20L }.sorted().toMutableList()
            if (restList.isNotEmpty() && restList.size % 2 == 0) {
                assertTrue(restList.sum() % 20L == 0L)
            }
        }
        kio.execute()
    }
}
