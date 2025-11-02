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

import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection

inline fun <T> PCollection<T>.that(): PAssert.IterableAssert<T> {
    return PAssert.that(this)
}

inline fun <T> PCollection<T>.that(reason: String): PAssert.IterableAssert<T> {
    return PAssert.that(reason, this)
}

inline fun <T> PCollection<T>.thatSingleton(): PAssert.SingletonAssert<T> {
    return PAssert.thatSingleton(this)
}

inline fun <T> PCollection<T>.thatSingleton(reason: String): PAssert.SingletonAssert<T> {
    return PAssert.thatSingleton(reason, this)
}

inline fun <T> PCollection<Iterable<T>>.thatSingletonIterable(): PAssert.IterableAssert<T> {
    return PAssert.thatSingletonIterable(this)
}

inline fun <T> PCollection<Iterable<T>>.thatSingletonIterable(reason: String): PAssert.IterableAssert<T> {
    return PAssert.thatSingletonIterable(reason, this)
}

inline fun <K, V> PCollection<KV<K, V>>.thatMap(): PAssert.SingletonAssert<Map<K, V>> {
    return PAssert.thatMap(this)
}

inline fun <K, V> PCollection<KV<K, V>>.thatMap(reason: String): PAssert.SingletonAssert<Map<K, V>> {
    return PAssert.thatMap(reason, this)
}

inline fun <K, V> PCollection<KV<K, V>>.thatMultiMap(): PAssert.SingletonAssert<Map<K, Iterable<V>>> {
    return PAssert.thatMultimap(this)
}

inline fun <K, V> PCollection<KV<K, V>>.thatMultiMap(reason: String): PAssert.SingletonAssert<Map<K, Iterable<V>>> {
    return PAssert.thatMultimap(reason, this)
}

inline fun <T> PAssert.SingletonAssert<T>.satisfy(crossinline checker: (T) -> Unit): PAssert.SingletonAssert<T> {
    return this.satisfies {
        checker(it)
        null
    }
}

inline fun <T> PAssert.IterableAssert<T>.satisfy(
    crossinline checker: (Iterable<T>) -> Unit
): PAssert.IterableAssert<T> {
    return this.satisfies {
        checker(it)
        null
    }
}
