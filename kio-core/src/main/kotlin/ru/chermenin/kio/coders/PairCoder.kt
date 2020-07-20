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

package ru.chermenin.kio.coders

import java.io.InputStream
import java.io.OutputStream
import org.apache.beam.sdk.coders.*
import org.apache.beam.sdk.values.TypeDescriptor

class PairCoder<K, V> private constructor(
    private val firstCoder: Coder<K>,
    private val secondCoder: Coder<V>
) : StructuredCoder<Pair<K, V>>() {

    class Provider : CoderProvider() {

        @Suppress("UNCHECKED_CAST")
        override fun <T> coderFor(
            typeDescriptor: TypeDescriptor<T>,
            componentCoders: MutableList<out Coder<*>>
        ): Coder<T> {
            if (typeDescriptor.rawType == Pair::class.java) {
                return of(componentCoders[0], componentCoders[1]) as Coder<T>
            } else {
                throw CannotProvideCoderException("Can not provide coder for type '${typeDescriptor.rawType}'")
            }
        }
    }

    companion object {
        fun <K, V> of(keyCoder: Coder<K>, valueCoder: Coder<V>): PairCoder<K, V> {
            return PairCoder(keyCoder, valueCoder)
        }
    }

    override fun encode(value: Pair<K, V>, outStream: OutputStream) {
        firstCoder.encode(value.first, outStream)
        secondCoder.encode(value.second, outStream)
    }

    override fun decode(inStream: InputStream): Pair<K, V> {
        val first = firstCoder.decode(inStream)
        val second = secondCoder.decode(inStream)
        return Pair(first, second)
    }

    override fun getCoderArguments(): MutableList<out Coder<*>> {
        return mutableListOf(firstCoder, secondCoder)
    }

    override fun verifyDeterministic() {
        verifyDeterministic(this, "First coder must be deterministic", firstCoder)
        verifyDeterministic(this, "Second coder must be deterministic", secondCoder)
    }
}
