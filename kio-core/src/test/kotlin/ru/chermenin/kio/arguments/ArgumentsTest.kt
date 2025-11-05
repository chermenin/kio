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

package ru.chermenin.kio.arguments

import org.apache.beam.runners.direct.DirectRunner
import org.junit.Assert.*
import org.junit.Test
import ru.chermenin.kio.utils.getVersion

class ArgumentsTest {

    @Test
    fun checkArguments() {
        val beamVersion = getVersion("beam")

        val args: Array<String> = arrayOf(
            "--runner=DirectRunner",
            "--jobName=TestJob",
            "--input=some/path/to/input.txt",
            "--output=some/other/path/to/output.csv",
            "--window=1800",
            "--fraction=0.782315",
            "--value=0.0314159265358e2",
            "--showLog=False",
            "--skipTests"
        )

        val (options, arguments) = Arguments.parse(args)

        // check options
        assertEquals(DirectRunner::class.java.name, options.runner.name)
        assertEquals("TestJob", options.jobName)
        assertEquals("Apache_Beam_SDK_for_Java/$beamVersion", options.userAgent)

        // check base string arguments
        assertEquals("some/path/to/input.txt", arguments.get("input"))
        assertEquals("some/other/path/to/output.csv", arguments["output"])
        assertEquals(null, arguments.optional("any_key"))
        assertThrows(IllegalArgumentException::class.java) { arguments.required("any_key") }
        assertEquals("default", arguments.getOrElse("any_key") { "default" })

        // check int
        assertEquals(1800, arguments.int("window"))
        assertThrows(IllegalArgumentException::class.java) { arguments.int("fraction") }
        assertThrows(IllegalArgumentException::class.java) { arguments.int("value") }

        // check long
        assertEquals(1800L, arguments.long("window"))
        assertThrows(IllegalArgumentException::class.java) { arguments.long("fraction") }
        assertThrows(IllegalArgumentException::class.java) { arguments.long("value") }

        // check float
        assertEquals(1800.0f, arguments.float("window"))
        assertEquals(0.782315f, arguments.float("fraction"))
        assertEquals(Math.PI.toFloat(), arguments.float("value"))

        // check double
        assertEquals(1800.0, arguments.double("window"), 0.0000001)
        assertEquals(0.782315, arguments.double("fraction"), 0.0000001)
        assertEquals(Math.PI, arguments.double("value"), 0.00000000001)

        // check boolean
        assertEquals(false, arguments.bool("showLog"))
        assertEquals(true, arguments.bool("skipTests"))
        assertEquals(false, arguments.bool("waitFinish"))

        // check arguments string
        assertEquals(
            """
                [--fraction=0.782315
                --input=some/path/to/input.txt
                --output=some/other/path/to/output.csv
                --showLog=False
                --skipTests=true
                --value=0.0314159265358e2
                --window=1800]
            """.trimIndent(),
            arguments.joinToString(separator = "\n", prefix = "[", suffix = "]")
        )
    }
}
