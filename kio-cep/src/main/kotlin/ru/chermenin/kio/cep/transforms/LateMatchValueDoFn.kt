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

package ru.chermenin.kio.cep.transforms

import java.io.Serializable
import org.apache.beam.sdk.coders.KvCoder
import org.apache.beam.sdk.coders.SerializableCoder
import org.apache.beam.sdk.coders.VarLongCoder
import org.apache.beam.sdk.state.*
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.KV
import org.joda.time.Instant
import ru.chermenin.kio.cep.ComplexEvent
import ru.chermenin.kio.cep.nfa.NFA
import ru.chermenin.kio.cep.pattern.Pattern

/**
 * Operator to match key-value pairs by pattern with allowed lateness.
 *
 * @param pattern pattern to match values
 * @param allowedLateness size of allowed lateness (milliseconds)
 * @param keyClass class of keys
 * @param valueClass class of values
 * @param K key type
 * @param V value type
 */
class LateMatchValueDoFn<K : Serializable, V : Serializable>(
    private val pattern: Pattern<V>,
    private val allowedLateness: Long,
    keyClass: Class<K>,
    valueClass: Class<V>
) : DoFn<KV<K, V>, KV<K, ComplexEvent<V>>>() {

    @StateId(NFA_STATE_KEY)
    val nfaState: StateSpec<ValueState<NFA<*>>> = StateSpecs.value(SerializableCoder.of(NFA::class.java))

    @StateId(BUFFER_STATE_KEY)
    val bufferState: StateSpec<BagState<KV<Long, KV<K, V>>>> = StateSpecs.bag(
        KvCoder.of(
            VarLongCoder.of(),
            KvCoder.of(SerializableCoder.of(keyClass), SerializableCoder.of(valueClass))
        )
    )

    @TimerId(LATENESS_TIMER_KEY)
    val latenessTimer: TimerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME)

    @Suppress("UNCHECKED_CAST")
    @ProcessElement
    fun processElement(
        context: ProcessContext,
        @StateId(NFA_STATE_KEY) nfaState: ValueState<NFA<*>>,
        @StateId(BUFFER_STATE_KEY) bufferState: BagState<KV<Long, KV<K, V>>>,
        @TimerId(LATENESS_TIMER_KEY) latenessTimer: Timer
    ) {
        val nfa = nfaState.read() ?: pattern.compile()
        nfaState.write(nfa)

        val bufferedEvents = bufferState.read().toList()
        bufferState.clear()

        val element = context.element()
        val maxTimestamp = bufferedEvents.map { it.key!! }.maxOrNull() ?: context.timestamp().millis
        val expireTimestamp = maxTimestamp - allowedLateness
        val (expiredEvents, bufferEvents) = bufferedEvents.partition { it.key!! <= expireTimestamp }
        bufferEvents.forEach { bufferState.add(it) }
        bufferState.add(KV.of(context.timestamp().millis, element))

        expiredEvents.sortedBy { it.key }.forEach {
            (nfa as NFA<V>).process(it.value.value, it.key!!).forEach {
                context.output(KV.of(element.key, ComplexEvent(it)))
            }
        }

        latenessTimer.set(Instant.ofEpochMilli(maxTimestamp + allowedLateness))
    }

    @Suppress("UNCHECKED_CAST")
    @OnTimer(LATENESS_TIMER_KEY)
    fun onExpire(
        context: OnTimerContext,
        @StateId(NFA_STATE_KEY) nfaState: ValueState<NFA<*>>,
        @StateId(BUFFER_STATE_KEY) bufferState: BagState<KV<Long, KV<K, V>>>
    ) {
        val nfa = nfaState.read()!!

        val bufferedEvents = bufferState.read().toList()
        bufferState.clear()

        if (bufferedEvents.isNotEmpty()) {
            val key = bufferedEvents[0].value.key!!
            bufferedEvents.sortedBy { it.key }.forEach {
                (nfa as NFA<V>).process(it.value.value, it.key!!).forEach {
                    context.output(KV.of(key, ComplexEvent(it)))
                }
            }
        }
    }
}
