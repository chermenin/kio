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

package ru.chermenin.kio.functions

import com.twitter.chill.ClosureCleaner
import kotlin.reflect.jvm.jvmName
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionTuple
import org.apache.beam.sdk.values.TupleTag
import org.apache.beam.sdk.values.TupleTagList
import ru.chermenin.kio.utils.hashWithName

class PCollectionWithSideOutput<T>(
    val collection: PCollection<T>,
    val sideOutputTags: List<TupleTag<*>>
)

inline fun <T> PCollection<T>.withSideOutputs(vararg sideOutputTag: TupleTag<*>): PCollectionWithSideOutput<T> {
    return PCollectionWithSideOutput<T>(this, sideOutputTag.toList())
}

inline fun <reified T, reified U> PCollectionWithSideOutput<T>.flatMap(
    noinline f: (T, DoFn<T, U>.ProcessContext) -> Iterable<U>
): Pair<PCollection<U>, PCollectionTuple> {
    val generalTag = TupleTag<U>()
    val mapper = ParDo.of(
        object : DoFn<T, U>() {
            private val g = ClosureCleaner.clean(f) // defeat closure

            @ProcessElement
            fun processElement(context: ProcessContext) {
                g(context.element(), context).forEach { context.output(it) }
            }
        }
    ).withOutputTags(generalTag, TupleTagList.of(sideOutputTags))
    val results = this.collection.apply(mapper.hashWithName("flatMapWithSideOutput(${f::class.jvmName})"), mapper)
    return Pair(results[generalTag], results)
}

inline fun <reified T, reified U> PCollectionWithSideOutput<T>.map(
    noinline f: (T, DoFn<T, U>.ProcessContext) -> U
): Pair<PCollection<U>, PCollectionTuple> {
    val generalTag = TupleTag<U>()
    val mapper = ParDo.of(
        object : DoFn<T, U>() {
            private val g = ClosureCleaner.clean(f) // defeat closure

            @ProcessElement
            fun processElement(context: ProcessContext) {
                context.output(g(context.element(), context))
            }
        }
    ).withOutputTags(generalTag, TupleTagList.of(sideOutputTags))
    val results = this.collection.apply(mapper.hashWithName("mapWithSideOutput(${f::class.jvmName})"), mapper)
    return Pair(results[generalTag], results)
}

inline fun <reified T> PCollection<T>.splitBy(noinline p: (T) -> Boolean): Pair<PCollection<T>, PCollection<T>> {
    val sideOutputTag = TupleTag<T>()
    val results = this.withSideOutputs(sideOutputTag).flatMap { element, context: DoFn<T, T>.ProcessContext ->
        if (p(element)) {
            listOf(element)
        } else {
            context.output(sideOutputTag, element)
            emptyList()
        }
    }
    results.first.name = p.hashWithName("partitionBy($p)")
    return Pair(results.first.setCoder(this.coder), results.second[sideOutputTag].setCoder(this.coder))
}
