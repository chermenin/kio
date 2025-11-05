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

import kotlin.reflect.jvm.jvmName
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.View
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionView
import ru.chermenin.kio.utils.ClosureCleaner
import ru.chermenin.kio.utils.hashWithName

class PCollectionWithSideInput<T>(
    val collection: PCollection<T>,
    val sideInputs: List<PCollectionView<*>>
)

inline fun <T> PCollection<T>.withSideInputs(vararg view: PCollectionView<*>): PCollectionWithSideInput<T> {
    return PCollectionWithSideInput(this, view.toList())
}

inline fun <T> PCollection<T>.asSingletonView(): PCollectionView<T> {
    return this.apply(View.asSingleton())
}

inline fun <T> PCollection<T>.asListView(): PCollectionView<List<T>> {
    return this.apply(View.asList())
}

inline fun <T> PCollection<T>.asIterableView(): PCollectionView<Iterable<T>> {
    return this.apply(View.asIterable())
}

inline fun <K, V> PCollection<KV<K, V>>.asMapView(): PCollectionView<Map<K, V>> {
    return this.apply(View.asMap())
}

inline fun <K, V> PCollection<KV<K, V>>.asMultiMapView(): PCollectionView<Map<K, Iterable<V>>> {
    return this.apply(View.asMultimap())
}

inline fun <T, U> PCollectionWithSideInput<T>.flatMap(
    f: KioFunction2<T, DoFn<T, U>.ProcessContext, Iterable<U>>
): PCollection<U> {
    val mapper = ParDo.of(
        object : DoFn<T, U>() {
            private val g = ClosureCleaner.clean(f)

            @ProcessElement
            fun processElement(context: ProcessContext) {
                g.invoke(context.element(), context).forEach { context.output(it) }
            }
        }
    ).withSideInputs(sideInputs)
    return collection.apply(mapper.hashWithName("flatMapWithSideInput(${f::class.jvmName})"), mapper)
}

inline fun <T, U> PCollectionWithSideInput<T>.map(
    f: KioFunction2<T, DoFn<T, U>.ProcessContext, U>
): PCollection<U> {
    val mapper = ParDo.of(
        object : DoFn<T, U>() {
            private val g = ClosureCleaner.clean(f)

            @ProcessElement
            fun processElement(context: ProcessContext) {
                context.output(g.invoke(context.element(), context))
            }
        }
    ).withSideInputs(sideInputs)
    return collection.apply(mapper.hashWithName("mapWithSideInput(${f::class.jvmName})"), mapper)
}
