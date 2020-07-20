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

package ru.chermenin.kio.it

import org.junit.Test
import ru.chermenin.kio.functions.*
import ru.chermenin.kio.test.*

class WordCountIntegrationTest : KioPipelineTest() {

    @Test
    fun wordCount() {

        // Create input file and get path
        val inputPath = "/data/wordCount/input.txt".asResourceFile { it.absolutePath }

        // Configure a pipeline
        kio.read().text(inputPath)
            .map { it.toLowerCase() }
            .flatMap { it.split("\\W+".toRegex()) }
            .filter { it.isNotEmpty() }
            .countByValue()
            .map { "${it.key},${it.value}" }
            .that().containsInAnyOrder(
                *"/data/wordCount/expected.txt".asResourceLines { it.toTypedArray() }
            )

        // And execute it
        kio.execute()
    }
}
