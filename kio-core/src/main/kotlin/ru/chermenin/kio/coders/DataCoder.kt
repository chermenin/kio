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

import java.io.*
import kotlin.jvm.internal.Reflection
import org.apache.beam.sdk.coders.*
import org.apache.beam.sdk.values.TypeDescriptor

class DataCoder<T> private constructor(private val dataClass: Class<T>) : Coder<T>() {

    class Provider : CoderProvider() {

        @Suppress("UNCHECKED_CAST")
        override fun <T> coderFor(
            typeDescriptor: TypeDescriptor<T>,
            componentCoders: MutableList<out Coder<*>>
        ): Coder<T> {
            val kotlinDataClass = Reflection.getOrCreateKotlinClass(typeDescriptor.rawType)
            if (kotlinDataClass.isData) {
                return of(typeDescriptor.rawType) as Coder<T>
            } else {
                throw CannotProvideCoderException("Can not provide coder for type '${typeDescriptor.rawType}'")
            }
        }
    }

    companion object {
        fun <T> of(dataClass: Class<T>): DataCoder<T> {
            return DataCoder(dataClass)
        }
    }

    override fun encode(value: T, outStream: OutputStream) {
        if (Serializable::class.java.isAssignableFrom(dataClass)) {
            val oos = ObjectOutputStream(outStream)
            oos.writeObject(value)
            oos.flush()
        } else {
            throw CoderException("Cannot encode a non-serializable data class")
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun decode(inStream: InputStream): T {
        val ois = ObjectInputStream(inStream)
        return ois.readObject() as T
    }

    override fun getCoderArguments(): MutableList<out Coder<*>> {
        return mutableListOf()
    }

    override fun verifyDeterministic() {}
}
