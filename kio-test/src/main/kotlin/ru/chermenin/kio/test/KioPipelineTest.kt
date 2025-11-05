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

package ru.chermenin.kio.test

import java.io.File
import java.util.logging.Logger
import org.apache.beam.sdk.testing.TestPipeline
import org.junit.After
import org.junit.Rule
import ru.chermenin.kio.Kio

abstract class KioPipelineTest {

    private val logger = Logger.getLogger(this::class.qualifiedName)

    @Rule
    @JvmField
    @Transient
    public val testPipeline: TestPipeline = TestPipeline.create()

    private val tempFiles = mutableListOf<File>()

    protected val kio: Kio = Kio.fromPipeline(testPipeline)

    @After
    fun clearTempFiles() {

        // try to delete temporary files
        if (tempFiles.isNotEmpty()) {
            tempFiles.removeIf { it.delete() }
        }

        // warn if still have temporary files
        if (tempFiles.isNotEmpty()) {
            logger.warning("Unable to delete the following temporary files: ${tempFiles.joinToString { it.name }}")
        }
    }

    fun <T> String.asResourceLines(process: (List<String>) -> T): T {
        val callerName = Thread.currentThread().stackTrace[2].className
        val content = Class.forName(callerName).getResource(this).readText().split("\n")
        return process(if (content[content.size - 1].isEmpty()) content.subList(0, content.size - 1) else content)
    }

    fun <T> String.asResourceFile(process: (File) -> T): T {
        val tempFile = File.createTempFile("kio-test", ".tmp")
        val callerName = Thread.currentThread().stackTrace[2].className
        val contentBytes = Class.forName(callerName).getResource(this).readBytes()
        tempFile.writeBytes(contentBytes)
        tempFiles.add(tempFile)
        return process(tempFile)
    }
}
