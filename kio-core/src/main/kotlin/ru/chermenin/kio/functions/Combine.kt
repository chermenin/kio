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

import org.apache.beam.sdk.coders.*
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.transforms.join.CoGroupByKey
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple
import org.apache.beam.sdk.transforms.windowing.GlobalWindows
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.TupleTag
import org.apache.beam.sdk.values.TypeDescriptor
import ru.chermenin.kio.coders.PairCoder
import ru.chermenin.kio.utils.ClosureCleaner
import ru.chermenin.kio.utils.hashWithName

const val COMBINER_BUFFER_MAX_SIZE = 20

inline fun <reified K, reified V, reified X> PCollection<KV<K, V>>.join(
    other: PCollection<KV<K, X>>
): PCollection<KV<K, Pair<@JvmSuppressWildcards V, @JvmSuppressWildcards X>>> {
    val (leftTag, rightTag) = Pair(TupleTag<V>("left"), TupleTag<X>("right"))
    return KeyedPCollectionTuple
        .of(leftTag, this)
        .and(rightTag, other)
        .apply(
            other.hashWithName("coGroup"),
            CoGroupByKey.create()
        )
        .flatMap {
            val lefts = it.value.getAll(leftTag)
            val rights = it.value.getAll(rightTag)
            lefts.flatMap { v -> rights.map { x -> KV.of(it.key, Pair(v, x)) } }
        }
}

inline fun <reified K, reified V, reified X> PCollection<KV<K, V>>.leftOuterJoin(
    other: PCollection<KV<K, X>>
): PCollection<KV<K, Pair<@JvmSuppressWildcards V, @JvmSuppressWildcards X?>>> {
    val (leftTag, rightTag) = Pair(TupleTag<V>("left"), TupleTag<X>("right"))
    return KeyedPCollectionTuple
        .of(leftTag, this)
        .and(rightTag, other)
        .apply(
            other.hashWithName("coGroup"),
            CoGroupByKey.create()
        )
        .flatMap {
            val lefts = it.value.getAll(leftTag)
            val rights = it.value.getAll(rightTag)
            lefts.flatMap { v -> (if (rights.none()) listOf(null) else rights).map { x -> KV.of(it.key, Pair(v, x)) } }
        }
}

inline fun <reified K, reified V, reified X> PCollection<KV<K, V>>.rightOuterJoin(
    other: PCollection<KV<K, X>>
): PCollection<KV<K, Pair<@JvmSuppressWildcards V?, @JvmSuppressWildcards X>>> {
    val (leftTag, rightTag) = Pair(TupleTag<V>("left"), TupleTag<X>("right"))
    return KeyedPCollectionTuple
        .of(leftTag, this)
        .and(rightTag, other)
        .apply(
            other.hashWithName("coGroup"),
            CoGroupByKey.create()
        )
        .flatMap {
            val lefts = it.value.getAll(leftTag)
            val rights = it.value.getAll(rightTag)
            (if (lefts.none()) listOf(null) else lefts).flatMap { v -> rights.map { x -> KV.of(it.key, Pair(v, x)) } }
        }
}

inline fun <reified K, reified V, reified X> PCollection<KV<K, V>>.fullOuterJoin(
    other: PCollection<KV<K, X>>
): PCollection<KV<K, Pair<@JvmSuppressWildcards V?, @JvmSuppressWildcards X?>>> {
    val (leftTag, rightTag) = Pair(TupleTag<V>("left"), TupleTag<X>("right"))
    return KeyedPCollectionTuple
        .of(leftTag, this)
        .and(rightTag, other)
        .apply(
            other.hashWithName("coGroup"),
            CoGroupByKey.create()
        )
        .flatMap {
            val lefts = it.value.getAll(leftTag)
            val rights = it.value.getAll(rightTag)
            (if (lefts.none()) listOf(null) else lefts).flatMap { v ->
                (if (rights.none()) listOf(null) else rights).map { x -> KV.of(it.key, Pair(v, x)) }
            }
        }
}

/**
 * Generic function to combine the elements for each key using a custom set of aggregation
 * functions.
 *
 * Users provide three functions:
 *  - `createCombiner`, which turns a V into a U (e.g., creates a one-element list)
 *  - `mergeValue`, to merge a V into a U (e.g., adds it to the end of a list)
 *  - `mergeCombiners`, to combine two U's into a single one.
 */
class Combiner<V, U>(
    createCombiner: KioFunction1<V, U>,
    mergeValue: KioFunction2<U, V, U>,
    mergeCombiners: KioFunction2<U, U, U>,
    private val elementCoder: Coder<V>,
    private val outputCoder: Coder<U>
) : Combine.CombineFn<V, Pair<MutableList<V>, U?>, U>() {

    // defeat closures
    private val cleanedCreateCombiner: KioFunction1<V, U> = ClosureCleaner.clean(createCombiner)
    private val cleanedMergeValue: KioFunction2<U, V, U> = ClosureCleaner.clean(mergeValue)
    private val cleanedMergeCombiners: KioFunction2<U, U, U> = ClosureCleaner.clean(mergeCombiners)

    private fun foldAccumulator(accumulator: Pair<MutableList<V>, U?>): U? {
        return if (accumulator.second != null) {
            accumulator.first.fold(accumulator.second!!) { acc: U, value: V ->
                cleanedMergeValue.invoke(acc, value)
            }
        } else {
            null
        }
    }

    override fun createAccumulator(): Pair<MutableList<V>, U?> {
        return Pair(mutableListOf(), null)
    }

    override fun addInput(mutableAccumulator: Pair<MutableList<V>, U?>, input: V): Pair<MutableList<V>, U?> {
        return if (mutableAccumulator.second == null) {
            Pair<MutableList<V>, U?>(mutableAccumulator.first, cleanedCreateCombiner.invoke(input))
        } else {
            val list = mutableAccumulator.first
            list.add(input)
            if (list.size >= COMBINER_BUFFER_MAX_SIZE) {
                Pair(mutableListOf(), foldAccumulator(mutableAccumulator))
            } else {
                mutableAccumulator
            }
        }
    }

    override fun mergeAccumulators(
        accumulators: MutableIterable<Pair<MutableList<V>, U?>>
    ): Pair<MutableList<V>, U?> {
        return accumulators.reduce { a, b ->
            val accA = foldAccumulator(a)
            val accB = foldAccumulator(b)
            val mergedAcc =
                if (accA == null) {
                    accB
                } else {
                    if (accB == null) {
                        accA
                    } else {
                        cleanedMergeCombiners.invoke(accA, accB)
                    }
                }
            Pair(mutableListOf(), mergedAcc)
        }
    }

    override fun extractOutput(accumulator: Pair<MutableList<V>, U?>): U? {
        return foldAccumulator(accumulator)
    }

    override fun getAccumulatorCoder(registry: CoderRegistry, inputCoder: Coder<V>): Coder<Pair<MutableList<V>, U?>> {
        return PairCoder.of(ListCoder.of(elementCoder), NullableCoder.of(getDefaultOutputCoder(registry, inputCoder)))
    }

    override fun getDefaultOutputCoder(registry: CoderRegistry, inputCoder: Coder<V>): Coder<U> {
        return outputCoder
    }
}

inline fun <T, reified U> PCollection<T>.combine(
    noinline createCombiner: (T) -> U,
    noinline mergeValue: (U, T) -> U,
    noinline mergeCombiners: (U, U) -> U,
    coder: Coder<U>? = null
): PCollection<U> {
    val outputCoder = coder ?: this.pipeline.coderRegistry.getCoder(TypeDescriptor.of(U::class.java))
    val combiner = Combiner(createCombiner, mergeValue, mergeCombiners, this.coder, outputCoder)
    return this.apply(combiner.hashWithName("combine"), Combine.globally<T, U>(combiner))
}

@Suppress("UNCHECKED_CAST")
inline fun <K, V, reified U> PCollection<KV<K, V>>.combineByKey(
    noinline createCombiner: (V) -> U,
    noinline mergeValue: (U, V) -> U,
    noinline mergeCombiners: (U, U) -> U,
    coder: Coder<U>? = null
): PCollection<KV<K, U>> {
    val outputCoder = coder ?: this.pipeline.coderRegistry.getCoder(TypeDescriptor.of(U::class.java))
    val combiner = Combiner(
        createCombiner,
        mergeValue,
        mergeCombiners,
        this.coder.coderArguments[1] as Coder<V>,
        outputCoder
    )
    return this.apply(combiner.hashWithName("combineByKey"), Combine.perKey<K, V, U>(combiner))
}

inline fun <T, reified U> PCollection<T>.aggregate(
    zeroValue: U,
    noinline seqOp: (U, T) -> U,
    noinline combOp: (U, U) -> U,
    coder: Coder<U>? = null
): PCollection<U> {
    return this.combine({ seqOp(zeroValue, it) }, seqOp, combOp, coder)
}

inline fun <K, V, reified U> PCollection<KV<K, V>>.aggregateByKey(
    zeroValue: U,
    noinline seqOp: (U, V) -> U,
    noinline combOp: (U, U) -> U,
    coder: Coder<U>? = null
): PCollection<KV<K, U>> {
    return this.combineByKey({ seqOp(zeroValue, it) }, seqOp, combOp, coder)
}

inline fun <reified T> PCollection<T>.fold(zeroValue: T, noinline f: (T, T) -> T): PCollection<T> {
    return this.combine({ f(zeroValue, it) }, f, f, this.coder)
}

@Suppress("UNCHECKED_CAST")
inline fun <K, reified V> PCollection<KV<K, V>>.foldByKey(
    zeroValue: V,
    noinline f: (V, V) -> V
): PCollection<KV<K, V>> {
    return this.combineByKey({ f(zeroValue, it) }, f, f, this.coder.coderArguments[1] as Coder<V>)
}

inline fun <reified T> PCollection<T>.reduce(noinline f: (T, T) -> T): PCollection<T> {
    return this.combine({ it }, f, f, this.coder)
}

@Suppress("UNCHECKED_CAST")
inline fun <K, reified V> PCollection<KV<K, V>>.reduceByKey(noinline f: (V, V) -> V): PCollection<KV<K, V>> {
    return this.combineByKey({ it }, f, f, this.coder.coderArguments[1] as Coder<V>)
}

inline fun <T : Comparable<T>> PCollection<T>.approximateQuantiles(numQuantiles: Int): PCollection<List<T>> {
    val quantiles = ApproximateQuantiles.globally<T>(numQuantiles)
    return this.apply(quantiles.hashWithName("approximateQuantiles($numQuantiles)"), quantiles)
}

inline fun <K, V : Comparable<V>> PCollection<KV<K, V>>.approximateQuantilesByKey(
    numQuantiles: Int
): PCollection<KV<K, List<V>>> {
    val quantiles = ApproximateQuantiles.perKey<K, V>(numQuantiles)
    return this.apply(quantiles.hashWithName("approximateQuantilesByKey($numQuantiles)"), quantiles)
}

inline fun <T> PCollection<T>.countApproxDistinct(maxEstimationError: Double): PCollection<Long> {
    val deduplicator = ApproximateUnique.globally<T>(maxEstimationError)
    return this.apply(deduplicator.hashWithName("countApproxDistinct($maxEstimationError)"), deduplicator)
}

inline fun <T> PCollection<T>.countApproxDistinct(sampleSize: Int): PCollection<Long> {
    val deduplicator = ApproximateUnique.globally<T>(sampleSize)
    return this.apply(deduplicator.hashWithName("countApproxDistinct($sampleSize)"), deduplicator)
}

inline fun <K, V> PCollection<KV<K, V>>.countApproxDistinctByKey(maxEstimationError: Double): PCollection<KV<K, Long>> {
    val deduplicator = ApproximateUnique.perKey<K, V>(maxEstimationError)
    return this.apply(deduplicator.hashWithName("countApproxDistinctByKey($maxEstimationError)"), deduplicator)
}

inline fun <K, V> PCollection<KV<K, V>>.countApproxDistinctByKey(sampleSize: Int): PCollection<KV<K, Long>> {
    val deduplicator = ApproximateUnique.perKey<K, V>(sampleSize)
    return this.apply(deduplicator.hashWithName("countApproxDistinctByKey($sampleSize)"), deduplicator)
}

inline fun <T> PCollection<T>.count(): PCollection<Long> {
    val counter = if (this.windowingStrategy.windowFn is GlobalWindows) {
        Count.globally<T>()
    } else {
        Combine.globally<T, Long>(Count.combineFn()).withoutDefaults()
    }
    return this.apply(counter.hashWithName("count"), counter)
}

inline fun <K, V> PCollection<KV<K, V>>.countByKey(): PCollection<KV<K, Long>> {
    val counter = Count.perKey<K, V>()
    return this.apply(counter.hashWithName("countByKey"), counter)
}

inline fun <T> PCollection<T>.countByValue(): PCollection<KV<T, Long>> {
    val counter = Count.perElement<T>()
    return this.apply(counter.hashWithName("countByValue"), counter)
}

inline fun <K, V> PCollection<KV<K, V>>.groupByKey(): PCollection<KV<K, Iterable<V>>> {
    val grouping = GroupByKey.create<K, V>()
    return this.apply(grouping.hashWithName("groupByKey"), grouping)
}

inline fun <T, reified K> PCollection<T>.groupBy(noinline f: (T) -> K): PCollection<KV<K, Iterable<T>>> {
    return this.keyBy(f).groupByKey()
}

inline fun <K, V> PCollection<KV<K, V>>.groupToBatches(size: Long): PCollection<KV<K, Iterable<V>>> {
    val grouping = GroupIntoBatches.ofSize<K, V>(size)
    return this.apply(grouping.hashWithName("groupToBatches($size)"), grouping)
}

inline fun <reified T> PCollection<T>.intersection(other: PCollection<T>): PCollection<T> {
    val (leftTag, rightTag) = Pair(TupleTag<Int>("left"), TupleTag<Int>("right"))
    return KeyedPCollectionTuple
        .of(leftTag, this.map { KV.of(it, 1) })
        .and(rightTag, other.map { KV.of(it, 1) })
        .apply(
            other.hashWithName("coGroup"),
            CoGroupByKey.create()
        )
        .filter { !(it.value.getAll(leftTag).none() || it.value.getAll(rightTag).none()) }
        .map { it.key!! }
}

inline fun <T> PCollection<T>.latest(): PCollection<T> {
    val combiner = if (this.windowingStrategy.windowFn is GlobalWindows) {
        Combine.globally(Latest.combineFn<T>())
    } else {
        Combine.globally(Latest.combineFn<T>()).withoutDefaults()
    }
    return this.asTimestamped().apply(combiner.hashWithName("latest"), combiner)
}

inline fun <K, V> PCollection<KV<K, V>>.latestByKey(): PCollection<KV<K, V>> {
    val combiner = Latest.perKey<K, V>()
    return this.apply(combiner.hashWithName("latestByKey"), combiner)
}

inline fun <T : Comparable<T>> PCollection<T>.max(): PCollection<T> {
    val combiner = if (this.windowingStrategy.windowFn is GlobalWindows) {
        Max.globally()
    } else {
        Combine.globally(Max.naturalOrder<T>()).withoutDefaults()
    }
    return this.apply(combiner.hashWithName("max"), combiner)
}

inline fun <K, V : Comparable<V>> PCollection<KV<K, V>>.maxByKey(): PCollection<KV<K, V>> {
    val combiner = Max.perKey<K, V>()
    return this.apply(combiner.hashWithName("maxByKey"), combiner)
}

inline fun <T : Number> PCollection<T>.mean(): PCollection<Double> {
    val combiner = if (this.windowingStrategy.windowFn is GlobalWindows) {
        Mean.globally()
    } else {
        Combine.globally(Mean.of<T>()).withoutDefaults()
    }
    return this.apply(combiner.hashWithName("mean"), combiner)
}

inline fun <K, V : Number> PCollection<KV<K, V>>.meanByKey(): PCollection<KV<K, Double>> {
    val combiner = Mean.perKey<K, V>()
    return this.apply(combiner.hashWithName("meanByKey"), combiner)
}

inline fun <T : Comparable<T>> PCollection<T>.min(): PCollection<T> {
    val combiner = if (this.windowingStrategy.windowFn is GlobalWindows) {
        Min.globally()
    } else {
        Combine.globally(Min.naturalOrder<T>()).withoutDefaults()
    }
    return this.apply(combiner.hashWithName("min"), combiner)
}

inline fun <K, V : Comparable<V>> PCollection<KV<K, V>>.minByKey(): PCollection<KV<K, V>> {
    val combiner = Min.perKey<K, V>()
    return this.apply(combiner.hashWithName("minByKey"), combiner)
}

inline fun <T> PCollection<T>.sample(sampleSize: Int): PCollection<Iterable<T>> {
    val sampler = Sample.fixedSizeGlobally<T>(sampleSize)
    return this.apply(sampler.hashWithName("sample($sampleSize)"), sampler)
}

inline fun <K, V> PCollection<KV<K, V>>.sampleByKey(sampleSize: Int): PCollection<KV<K, Iterable<V>>> {
    val sampler = Sample.fixedSizePerKey<K, V>(sampleSize)
    return this.apply(sampler.hashWithName("sampleByKey($sampleSize)"), sampler)
}

inline fun <reified T> PCollection<T>.subtract(other: PCollection<T>): PCollection<T> {
    val (leftTag, rightTag) = Pair(TupleTag<Int>("left"), TupleTag<Int>("right"))
    return KeyedPCollectionTuple
        .of(leftTag, this.map { KV.of(it, 1) })
        .and(rightTag, other.map { KV.of(it, 1) })
        .apply(
            other.hashWithName("coGroup"),
            CoGroupByKey.create()
        )
        .filter { !it.value.getAll(leftTag).none() && it.value.getAll(rightTag).none() }
        .map { it.key!! }
}

inline fun <reified K, reified V, reified X> PCollection<KV<K, V>>.subtractByKey(
    other: PCollection<KV<K, X>>
): PCollection<KV<K, V>> {
    val (leftTag, rightTag) = Pair(TupleTag<V>("left"), TupleTag<X>("right"))
    return KeyedPCollectionTuple
        .of(leftTag, this)
        .and(rightTag, other)
        .apply(
            other.hashWithName("coGroup"),
            CoGroupByKey.create()
        )
        .flatMap { pair ->
            if (!pair.value.getAll(leftTag).none() && pair.value.getAll(rightTag).none()) {
                pair.value.getAll(leftTag).map { KV.of(pair.key, it) }
            } else {
                emptyList()
            }
        }
}

@JvmName("sumFloats")
inline fun PCollection<Float>.sum(): PCollection<Float> {
    val adder = if (this.windowingStrategy.windowFn is GlobalWindows) {
        Sum.doublesGlobally()
    } else {
        Combine.globally(Sum.ofDoubles()).withoutDefaults()
    }
    return this
        .map { it.toDouble() }
        .apply(adder.hashWithName("sum"), adder)
        .map { it.toFloat() }
}

@JvmName("sumFloatsByKey")
inline fun <K> PCollection<KV<K, Float>>.sumByKey(): PCollection<KV<K, Float>> {
    val adder = Sum.doublesPerKey<K>()
    return this
        .map { KV.of(it.key, it.value.toDouble()) }
        .apply(adder.hashWithName("sum"), adder)
        .map { KV.of(it.key, it.value.toFloat()) }
}

@JvmName("sumDouble")
inline fun PCollection<Double>.sum(): PCollection<Double> {
    val adder = if (this.windowingStrategy.windowFn is GlobalWindows) {
        Sum.doublesGlobally()
    } else {
        Combine.globally(Sum.ofDoubles()).withoutDefaults()
    }
    return this.apply(adder.hashWithName("sum"), adder)
}

@JvmName("sumDoubleByKey")
inline fun <K> PCollection<KV<K, Double>>.sumByKey(): PCollection<KV<K, Double>> {
    val adder = Sum.doublesPerKey<K>()
    return this.apply(adder.hashWithName("sum"), adder)
}

@JvmName("sumInt")
inline fun PCollection<Int>.sum(): PCollection<Int> {
    val adder = if (this.windowingStrategy.windowFn is GlobalWindows) {
        Sum.integersGlobally()
    } else {
        Combine.globally(Sum.ofIntegers()).withoutDefaults()
    }
    return this.apply(adder.hashWithName("sum"), adder)
}

@JvmName("sumIntByKey")
inline fun <K> PCollection<KV<K, Int>>.sumByKey(): PCollection<KV<K, Int>> {
    val adder = Sum.integersPerKey<K>()
    return this.apply(adder.hashWithName("sum"), adder)
}

@JvmName("sumLong")
inline fun PCollection<Long>.sum(): PCollection<Long> {
    val adder = if (this.windowingStrategy.windowFn is GlobalWindows) {
        Sum.longsGlobally()
    } else {
        Combine.globally(Sum.ofLongs()).withoutDefaults()
    }
    return this.apply(adder.hashWithName("sum"), adder)
}

@JvmName("sumLongByKey")
inline fun <K> PCollection<KV<K, Long>>.sumByKey(): PCollection<KV<K, Long>> {
    val adder = Sum.longsPerKey<K>()
    return this.apply(adder.hashWithName("sum"), adder)
}

inline fun <T : Comparable<T>> PCollection<T>.top(count: Int): PCollection<List<T>> {
    val topFunction = if (this.windowingStrategy.windowFn is GlobalWindows) {
        Top.largest(count)
    } else {
        Combine.globally(Top.largestFn<T>(count)).withoutDefaults()
    }
    return this.apply(topFunction.hashWithName("top($count)"), topFunction)
}

inline fun <K, V : Comparable<V>> PCollection<KV<K, V>>.topByKey(count: Int): PCollection<KV<K, List<V>>> {
    val topFunction = Top.largestPerKey<K, V>(count)
    return this.apply(topFunction.hashWithName("topByKey($count)"), topFunction)
}

inline fun <T : Comparable<T>> PCollection<T>.largest(count: Int): PCollection<List<T>> {
    return this.top(count)
}

inline fun <K, V : Comparable<V>> PCollection<KV<K, V>>.largestByKey(count: Int): PCollection<KV<K, List<V>>> {
    return this.topByKey(count)
}

inline fun <T : Comparable<T>> PCollection<T>.smallest(count: Int): PCollection<List<T>> {
    val topFunction = if (this.windowingStrategy.windowFn is GlobalWindows) {
        Top.smallest(count)
    } else {
        Combine.globally(Top.smallestFn<T>(count)).withoutDefaults()
    }
    return this.apply(topFunction.hashWithName("top($count)"), topFunction)
}

inline fun <K, V : Comparable<V>> PCollection<KV<K, V>>.smallestByKey(count: Int): PCollection<KV<K, List<V>>> {
    val topFunction = Top.smallestPerKey<K, V>(count)
    return this.apply(topFunction.hashWithName("topByKey($count)"), topFunction)
}
