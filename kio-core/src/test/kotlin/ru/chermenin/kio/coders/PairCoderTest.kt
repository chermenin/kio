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

package ru.chermenin.kio.coders

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.coders.VarIntCoder
import org.junit.Assert
import org.junit.Test

class PairCoderTest {

    @Test
    fun checkPairCoder() {
        val pair = 42 to "answer"
        val coder = PairCoder.of(VarIntCoder.of(), StringUtf8Coder.of())

        // encode
        val inputStream = ByteArrayOutputStream()
        coder.encode(pair, inputStream)

        // decode
        val outputStream = ByteArrayInputStream(inputStream.toByteArray())
        val copy = coder.decode(outputStream)

        // both must be equal
        Assert.assertEquals(pair, copy)
    }
}
