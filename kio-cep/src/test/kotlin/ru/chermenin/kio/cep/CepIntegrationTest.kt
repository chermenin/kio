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

package ru.chermenin.kio.cep

import java.io.Serializable
import org.apache.beam.sdk.values.PCollection
import org.joda.time.Duration
import org.joda.time.Instant
import org.junit.Test
import ru.chermenin.kio.cep.pattern.Pattern
import ru.chermenin.kio.functions.*
import ru.chermenin.kio.test.*

/**
 * Integration test case to check the CEP module.
 */
class CepIntegrationTest : KioPipelineTest() {

    data class Event(val id: Int, val name: String, val value: Double) : Serializable

    @Test
    fun testSimplePattern() {
        val inputCollection: PCollection<Event> = kio.parallelize(listOf(
            Event(1, "barfoo", 1.0),
            Event(2, "start", 2.0),
            Event(3, "foobar", 3.0),
            Event(4, "foo", 4.0),
            Event(5, "middle", 5.0),
            Event(6, "middle", 6.0),
            Event(7, "bar", 3.0),
            Event(8, "end", 1.0),
            Event(9, "other", 2.0),
            Event(10, "bar", 4.0),
            Event(11, "barfoo", 1.0),
            Event(12, "start", 2.0),
            Event(13, "foobar", 3.0),
            Event(14, "foo", 4.0),
            Event(15, "middle", 5.0),
            Event(16, "middle", 6.0),
            Event(17, "bar", 3.0),
            Event(18, "end", 1.0),
            Event(19, "other", 2.0),
            Event(20, "bar", 4.0),
            Event(21, "barfoo", 1.0),
            Event(22, "start", 2.0),
            Event(23, "foobar", 3.0),
            Event(24, "foo", 4.0),
            Event(25, "middle", 5.0),
            Event(26, "middle", 6.0),
            Event(27, "bar", 3.0),
            Event(28, "end", 1.0),
            Event(29, "other", 2.0),
            Event(30, "bar", 4.0),
            Event(31, "foo", 42.0)
        ))

        val pattern: Pattern<Event> = Pattern
            .startWith<Event>("start") { it.name == "start" }
            .thenFollowByAny("middle") { it.name == "middle" && it.value > 5.5 }
            .thenFollowByAny("end") { it.name == "end" }
            .within(Duration.standardSeconds(10))

        inputCollection
            .withTimestamps {
                Instant.ofEpochMilli(Instant.now().millis + it.id * 1000 - 1000000)
            }
            .match(pattern, allowedLateness = Duration.standardSeconds(30))
            .map {
                val start = it["start"].elementAt(0)
                val middle = it["middle"].elementAt(0)
                val end = it["end"].elementAt(0)
                "${start.id} -> ${middle.id} -> ${end.id}"
            }
            .that().containsInAnyOrder(
                "2 -> 6 -> 8",
                "12 -> 16 -> 18",
                "22 -> 26 -> 28"
            )

        kio.execute().waitUntilDone()
    }
}
