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
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.joda.time.Duration
import ru.chermenin.kio.cep.pattern.Pattern
import ru.chermenin.kio.cep.transforms.LateMatchValueDoFn
import ru.chermenin.kio.cep.transforms.MatchValueDoFn
import ru.chermenin.kio.functions.keyBy
import ru.chermenin.kio.functions.values
import ru.chermenin.kio.utils.hashWithName

/**
 * A definition of the complex event for results.
 */
class ComplexEvent<T>(private val eventsMap: Map<String, Iterable<T>>) : Serializable {

    /**
     * Method to get events by name of the pattern element.
     *
     * @name the name of the pattern element
     * @return iterable of matched events
     */
    operator fun get(name: String): Iterable<T> {
        return eventsMap[name] ?: listOf()
    }

    override fun toString(): String {
        return "ComplexEvent($eventsMap)"
    }
}

/**
 * Method to match events by pattern.
 *
 * @param pattern pattern to match
 * @param allowedLateness the size of allowed lateness (optional)
 * @return collection of complex events
 */
inline fun <reified T : Serializable> PCollection<T>.match(
    pattern: Pattern<T>,
    allowedLateness: Duration = Duration.ZERO
): PCollection<ComplexEvent<T>> {
    return this.keyBy { "" }.matchValues(pattern, allowedLateness).values()
}

/**
 * Method to match key-value pairs by pattern.
 *
 * @param pattern pattern to match values
 * @param allowedLateness the size of allowed lateness (optional)
 * @return collection of complex events by keys
 */
inline fun <reified K : Serializable, reified V : Serializable> PCollection<KV<K, V>>.matchValues(
    pattern: Pattern<V>,
    allowedLateness: Duration = Duration.ZERO
): PCollection<KV<K, ComplexEvent<V>>> {
    return if (allowedLateness == Duration.ZERO) {
        this.apply(
            pattern.hashWithName("CEP.matchValues($pattern)"),
            CEP.matchValues(pattern)
        )
    } else {
        this.apply(
            pattern.hashWithName("CEP.lateMatchValues($pattern, $allowedLateness)"),
            CEP.lateMatchValues(pattern, allowedLateness, K::class.java, V::class.java)
        )
    }
}

/**
 * Object to allow applying patterns to collections using Java SDK.
 */
@Suppress("UNCHECKED_CAST")
object CEP {

    fun <K, V : Serializable> matchValues(
        pattern: Pattern<V>
    ): PTransform<PCollection<KV<K, V>>, PCollection<KV<K, ComplexEvent<V>>>> {
        return ParDo.of(MatchValueDoFn<K, V>(pattern))
            as PTransform<PCollection<KV<K, V>>, PCollection<KV<K, ComplexEvent<V>>>>
    }

    fun <K : Serializable, V : Serializable> lateMatchValues(
        pattern: Pattern<V>,
        allowedLateness: Duration,
        keyClass: Class<K>,
        valueClass: Class<V>
    ): PTransform<PCollection<KV<K, V>>, PCollection<KV<K, ComplexEvent<V>>>> {
        return ParDo.of(LateMatchValueDoFn(pattern, allowedLateness.millis, keyClass, valueClass))
            as PTransform<PCollection<KV<K, V>>, PCollection<KV<K, ComplexEvent<V>>>>
    }
}
