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

package ru.chermenin.kio.cep.pattern

import org.joda.time.Duration
import org.junit.Assert.*
import org.junit.Test

/**
 * Test class to check patterns.
 */
class PatternTest {

    @Test
    fun checkStrictTimePattern() {
        val pattern = Pattern
            .startWith<Any>("start")
            .then("next")
            .then("end")
            .within(Duration.standardSeconds(10))

        val parentPattern = pattern.parent
        val parentParentPattern = parentPattern?.parent

        assertNotNull(parentPattern)
        assertNotNull(parentParentPattern)
        assertNull(parentParentPattern?.parent)

        assertEquals(pattern.name, "end")
        assertEquals(parentPattern?.name, "next")
        assertEquals(parentParentPattern?.name, "start")

        assertEquals(pattern.consuming, Pattern.Consuming.NEXT)
        assertEquals(parentPattern?.consuming, Pattern.Consuming.NEXT)

        assertTrue(pattern is TimePattern)
        assertEquals(10L, (pattern as TimePattern).window.standardSeconds)
    }

    @Test
    fun checkNonStrictWindowPattern() {
        val pattern = Pattern
            .startWith<Any>("start")
            .thenFollowByAny("next")
            .thenFollowByAny("end")
            .withinWindow()

        val parentPattern = pattern.parent
        val parentParentPattern = parentPattern?.parent

        assertNotNull(parentPattern)
        assertNotNull(parentParentPattern)
        assertNull(parentParentPattern?.parent)

        assertEquals("end", pattern.name)
        assertEquals("next", parentPattern?.name)
        assertEquals("start", parentParentPattern?.name)

        assertEquals(pattern.consuming, Pattern.Consuming.FOLLOW_BY_ANY)
        assertEquals(parentPattern?.consuming, Pattern.Consuming.FOLLOW_BY_ANY)

        assertTrue(pattern is WindowPattern)
    }
}
