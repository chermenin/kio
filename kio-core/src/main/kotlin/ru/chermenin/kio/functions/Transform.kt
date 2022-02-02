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
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.coders.KvCoder
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.values.*
import org.joda.time.Instant
import ru.chermenin.kio.coders.PairCoder
import ru.chermenin.kio.utils.hashWithName

inline fun <T, U> PCollection<T>.transform(noinline f: (PCollection<T>) -> PCollection<U>): PCollection<U> {
    return this.transform(f.hashWithName("transform(${f::class.jvmName})"), f)
}

inline fun <T, U> PCollection<T>.transform(
    name: String,
    noinline f: (PCollection<T>) -> PCollection<U>
): PCollection<U> {
    return this.apply(name, object : PTransform<PCollection<T>, PCollection<U>>() {
        private val g = ClosureCleaner.clean(f) // defeat closure

        override fun expand(input: PCollection<T>): PCollection<U> {
            return g(input)
        }
    })
}

inline fun <T> PCollection<T>.distinct(): PCollection<T> {
    val deduplicator = Distinct.create<T>()
    return this.apply(deduplicator.hashWithName("distinct"), deduplicator)
}

inline fun <T> PCollection<T>.filter(noinline f: (T) -> Boolean): PCollection<T> {
    return this.apply(f.hashWithName("filter(${f::class.jvmName})"), ParDo.of(
        object : DoFn<T, T>() {
            private val g = ClosureCleaner.clean(f) // defeat closure

            @ProcessElement
            fun processElement(context: ProcessContext) {
                val element = context.element()
                if (g(element)) {
                    context.output(element)
                }
            }
        }
    ))
}

inline fun <reified T, U> PCollection<T>.flatMap(noinline f: (T) -> Iterable<U>): PCollection<U> {
    return this.apply(f.hashWithName("flatMap(${f::class.jvmName})"), ParDo.of(
        object : DoFn<T, U>() {
            private val g = ClosureCleaner.clean(f) // defeat closure

            @ProcessElement
            fun processElement(context: ProcessContext) {
                g(context.element()).forEach { context.output(it) }
            }
        }
    ))
}

inline fun <K, V, U> PCollection<KV<K, V>>.flatMapValues(noinline f: (V) -> Iterable<U>): PCollection<KV<K, U>> {
    return this
        .flatMap { kv -> f(kv.value).map { KV.of(kv.key, it) } }
        .setName(f.hashWithName("flatMapValues(${f::class.jvmName})"))
}

inline fun <T> PCollection<T>.forEach(noinline f: (T) -> Unit) {
    this.apply(f.hashWithName("forEach(${f::class.jvmName})"), ParDo.of(
        object : DoFn<T, Void>() {
            private val g = ClosureCleaner.clean(f) // defeat closure

            @ProcessElement
            fun processElement(context: ProcessContext) {
                g(context.element())
            }
        }
    ))
}

inline fun <T, K> PCollection<T>.keyBy(noinline f: (T) -> K): PCollection<KV<K, T>> {
    return this.apply(f.hashWithName("keyBy(${f::class.jvmName})"), ParDo.of(
        object : DoFn<T, KV<K, T>>() {
            private val g = ClosureCleaner.clean(f) // defeat closure

            @ProcessElement
            fun processElement(context: ProcessContext) {
                val element = context.element()
                context.output(KV.of(g(element), element))
            }
        }
    ))
}

inline fun <K, V> PCollection<KV<K, V>>.keys(): PCollection<K> {
    val keyExtractor: (KV<K, V>) -> K = { it.key!! }
    return this.apply(keyExtractor.hashWithName("keys"), ParDo.of(
        object : DoFn<KV<K, V>, K>() {
            private val g = ClosureCleaner.clean(keyExtractor) // defeat closure

            @ProcessElement
            fun processElement(context: ProcessContext) {
                context.output(g(context.element()))
            }
        }
    ))
}

inline fun <reified T, reified U> PCollection<T>.map(noinline f: (T) -> U): PCollection<U> {
    return this.apply(f.hashWithName("map(${f::class.jvmName})"), ParDo.of(
        object : DoFn<T, U>() {
            private val g = ClosureCleaner.clean(f) // defeat closure

            @ProcessElement
            fun processElement(context: ProcessContext) {
                context.output(g(context.element()))
            }
        }
    ))
}

inline fun <K, V, U> PCollection<KV<K, V>>.mapValues(noinline f: (V) -> U): PCollection<KV<K, U>> {
    return this
        .flatMapValues { listOf(f(it)) }
        .setName(f.hashWithName("mapValues(${f::class.jvmName})"))
}

inline fun PCollection<String>.parseJson(schema: Schema): PCollection<Row> {
    val parser = JsonToRow.withSchema(schema)
    return this.apply(parser.hashWithName("parseJson($schema)"), parser)
}

inline fun <T> PCollection<T>.partition(partitions: Int): PCollectionList<T> {
    val partitioner = Partition.of(
        partitions,
        Partition.PartitionFn<T> { element, numPartitions -> element.hashCode() % numPartitions }
    )
    return this.apply(partitioner.hashWithName("partition($partitions)"), partitioner)
}

inline fun <T> PCollection<T>.partitionBy(partitions: Int, noinline f: (T) -> Int): PCollectionList<T> {
    val partitioner = Partition.of(partitions, object : Partition.PartitionFn<T> {

        private val g = ClosureCleaner.clean(f) // defeat closure

        override fun partitionFor(element: T, numPartitions: Int): Int {
            return g(element) % numPartitions
        }
    })
    return this.apply(partitioner.hashWithName("partitionBy($partitions, $f)"), partitioner)
}

inline fun <T> PCollection<T>.peek(noinline f: (T) -> Unit): PCollection<T> {
    return this.apply(f.hashWithName("peek(${f::class.jvmName})"), ParDo.of(
        object : DoFn<T, T>() {
            private val g = ClosureCleaner.clean(f) // defeat closure

            @ProcessElement
            fun processElement(context: ProcessContext) {
                val element = context.element()
                g(element)
                context.output(element)
            }
        }
    ))
}

@Suppress("UNCHECKED_CAST")
inline fun <reified K, reified V> PCollection<KV<K, V>>.swap(): PCollection<KV<V, K>> {
    val pairValueCoders = this.coder.coderArguments
    val mapper: (KV<K, V>) -> KV<V, K> = { KV.of(it.value, it.key) }
    return this
        .map(mapper)
        .setName(mapper.hashWithName("swap"))
        .setCoder(KvCoder.of(pairValueCoders[1] as Coder<V>, pairValueCoders[0] as Coder<K>))
}

inline fun <T> PCollection<T>.take(limit: Long): PCollection<T> {
    val sampler = Sample.any<T>(limit)
    return this.apply(sampler.hashWithName("take($limit)"), sampler)
}

inline fun <T> PCollection<T>.toJson(): PCollection<String> {
    val formatter = ToJson.of<T>()
    return this.apply(formatter.hashWithName("toJson"), formatter)
}

@Suppress("UNCHECKED_CAST")
inline fun <K, V> PCollection<Pair<K, V>>.toKV(): PCollection<KV<K, V>> {
    val pairValueCoders = this.coder.coderArguments
    val mapper: (Pair<K, V>) -> KV<K, V> = { KV.of(it.first, it.second) }
    return this
        .map(mapper)
        .setName(mapper.hashWithName("toKV"))
        .setCoder(KvCoder.of(pairValueCoders[0] as Coder<K>, pairValueCoders[1] as Coder<V>))
}

@Suppress("UNCHECKED_CAST")
fun <K, V> PCollection<KV<K, V>>.toPair(): PCollection<Pair<K, V>> {
    val keyValueCoders = this.coder.coderArguments
    val mapper: (KV<K, V>) -> Pair<K, V> = { Pair(it.key!!, it.value) }
    return this.map(mapper)
        .setName(mapper.hashWithName("toPair"))
        .setCoder(PairCoder.of(keyValueCoders[0] as Coder<K>, keyValueCoders[1] as Coder<V>))
}

inline fun <K, V> PCollection<KV<K, V>>.values(): PCollection<V> {
    val valueExtractor: (KV<K, V>) -> V = { it.value }
    return this.apply(valueExtractor.hashWithName("values"), ParDo.of(
        object : DoFn<KV<K, V>, V>() {
            private val g = ClosureCleaner.clean(valueExtractor) // defeat closure

            @ProcessElement
            fun processElement(context: ProcessContext) {
                context.output(g(context.element()))
            }
        }
    ))
}

inline fun <T> PCollection<T>.withTimestamps(noinline f: (T) -> Instant): PCollection<T> {
    val timestampExtractor = WithTimestamps.of(object : SerializableFunction<T, Instant> {
        private val g = ClosureCleaner.clean(f) // defeat closure
        override fun apply(element: T): Instant {
            return g(element)
        }
    })
    return this.apply(timestampExtractor.hashWithName("withTimestamps(${f::class.jvmName})"), timestampExtractor)
}

fun <T> PCollection<T>.and(pc: PCollection<T>): PCollectionList<T> {
    return PCollectionList.of(this).and(pc)
}

fun <T> PCollectionList<T>.flatten(): PCollection<T> {
    val flatter = Flatten.pCollections<T>()
    return this.apply(flatter.hashWithName("flatten"), flatter)
}

fun <T> PCollection<T>.union(pc: PCollection<T>): PCollection<T> {
    return this.and(pc).flatten()
}
