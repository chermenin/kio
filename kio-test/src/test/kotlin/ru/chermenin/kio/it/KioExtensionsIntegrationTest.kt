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

package ru.chermenin.kio.it

import java.io.Serializable
import org.junit.Test
import ru.chermenin.kio.functions.*
import ru.chermenin.kio.test.*

class KioExtensionsIntegrationTest : KioPipelineTest() {

    @Test
    fun testPipeline() {

        // Create inputs
        val input1 = kio.parallelize(listOf(1, 2, 3))
        val input2 = kio.parallelize(listOf(4, 5, 6))

        // Configure pipelines
        val results = input1.union(input2)
            .filter { it % 2 > 0 }
            .flatMap { num -> arrayOfNulls<String>(num).map { String(charArrayOf((num + 42).toChar())) } }
            .map { String(charArrayOf(it[0])) }
            .keyBy { it[0].code }
            .groupByKey()
            .toPair()

        // Assert results
        results.that().containsInAnyOrder(
            Pair(43, arrayOf("+").asIterable()),
            Pair(45, arrayOf("-", "-", "-").asIterable()),
            Pair(47, arrayOf("/", "/", "/", "/", "/").asIterable())
        )

        // Execute pipeline
        kio.execute().waitUntilDone()
    }

    @Test
    fun testSql() {
        data class Person(val id: Int, val name: String, val age: Int) : Serializable

        kio
            .parallelize(listOf(
                Person(0, "John", 26),
                Person(1, "Peter", 13),
                Person(2, "Tim", 64),
                Person(3, "Jane", 31),
                Person(4, "Bill", 17)
            ))
            .toRows()
            .sql("SELECT id FROM PCOLLECTION WHERE age >= 18")
            .map { it.getValue<Int>("id") }
            .that().containsInAnyOrder(0, 2, 3)

        kio.execute().waitUntilDone()
    }

    @Test
    fun testJoin() {

        data class Person(val id: Int, val name: String, val age: Int, val cityId: Int) : Serializable

        data class City(val id: Int, val name: String) : Serializable

        val persons = kio
            .parallelize(listOf(
                Person(0, "John", 26, 2),
                Person(1, "Peter", 13, 0),
                Person(2, "Tim", 64, 1),
                Person(3, "Jane", 31, 1),
                Person(4, "Bill", 17, 2)
            ))

        val cities = kio
            .parallelize(listOf(
                City(0, "Moscow"),
                City(1, "Paris"),
                City(2, "London"),
                City(4, "Tokyo")
            ))

        with(
            persons.toRows() to "persons",
            cities.toRows() to "cities"
        )
            .sql("""
                SELECT p.name
                FROM persons p
                JOIN cities c
                  ON p.cityId = c.id
                WHERE c.name = 'Paris'
            """)
            .map { it.getValue<String>("name") }
            .that().containsInAnyOrder("Tim", "Jane")

        kio.execute().waitUntilDone()
    }
}
