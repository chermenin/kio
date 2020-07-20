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
import org.apache.beam.sdk.coders.SerializableCoder
import org.apache.beam.sdk.state.StateSpec
import org.apache.beam.sdk.state.StateSpecs
import org.apache.beam.sdk.state.ValueState
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.KV
import ru.chermenin.kio.cep.ComplexEvent
import ru.chermenin.kio.cep.nfa.NFA
import ru.chermenin.kio.cep.pattern.Pattern

/**
 * Operator to match key-value pairs by pattern.
 *
 * @param pattern pattern to match values
 * @param K key type
 * @param V value type
 */
class MatchValueDoFn<K, V : Serializable>(private val pattern: Pattern<V>) : DoFn<KV<K, V>, KV<K, ComplexEvent<V>>>() {

    @StateId(NFA_STATE_KEY)
    val nfaState: StateSpec<ValueState<NFA<*>>>? = StateSpecs.value(SerializableCoder.of(NFA::class.java))

    @Suppress("UNCHECKED_CAST")
    @ProcessElement
    fun processElement(
        context: ProcessContext,
        @StateId(NFA_STATE_KEY) nfaState: ValueState<NFA<*>>
    ) {
        val nfa = nfaState.read() ?: pattern.compile()
        nfaState.write(nfa)
        val element = context.element()
        (nfa as NFA<V>).process(element.value, context.timestamp().millis).forEach {
            context.output(KV.of(element.key, ComplexEvent(it)))
        }
    }
}
