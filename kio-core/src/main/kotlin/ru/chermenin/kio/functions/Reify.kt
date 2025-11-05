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

package ru.chermenin.kio.functions

import org.apache.beam.sdk.transforms.Reify
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.TimestampedValue
import org.apache.beam.sdk.values.ValueInSingleWindow
import ru.chermenin.kio.utils.hashWithName

inline fun <T> PCollection<T>.asTimestamped(): PCollection<TimestampedValue<T>> {
    val reifier = Reify.timestamps<T>()
    return this.apply(reifier.hashWithName("asTimestamped"), reifier)
}

inline fun <K, V> PCollection<KV<K, V>>.asTimestampedValues(): PCollection<KV<K, TimestampedValue<V>>> {
    val reifier = Reify.timestampsInValue<K, V>()
    return this.apply(reifier.hashWithName("asTimestampedValues"), reifier)
}

inline fun <T> PCollection<T>.asWindowed(): PCollection<ValueInSingleWindow<T>> {
    val reifier = Reify.windows<T>()
    return this.apply(reifier.hashWithName("asWindowed"), reifier)
}

inline fun <K, V> PCollection<KV<K, V>>.asWindowedValues(): PCollection<KV<K, ValueInSingleWindow<V>>> {
    val reifier = Reify.windowsInValue<K, V>()
    return this.apply(reifier.hashWithName("asWindowedValues"), reifier)
}
