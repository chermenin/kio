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

package ru.chermenin.kio.cep.nfa

import java.io.Serializable
import org.joda.time.Duration

/**
 * An implementation of non-deterministic finite automaton for CEP.
 *
 * @param beginningState the beginning state of the NFA
 * @param window length of the window (optional)
 * @param T Type of the processed events
 */
class NFA<T> internal constructor(
    internal val beginningState: State<T>,
    internal val window: Duration?
) : Serializable {

    private var internalStates: List<InternalState<T>> = emptyList()

    /**
     * Process current event and get results if some of the computations reach a final state.
     *
     * @param event the current event
     * @param timestamp the timestamp of the current event
     * @return iterable of matched entries
     */
    fun process(event: T, timestamp: Long): Iterable<Map<String, Iterable<T>>> {

        val (updatedStates, results) = internalStates
            .fold(Pair(emptyList<InternalState<T>>(), emptyList<Map<String, Iterable<T>>>())) { acc, state ->
                val nextStates = if (
                    state.currentState.type != State.Type.START &&
                    window != null &&
                    timestamp - state.startTimestamp >= window.millis
                ) {
                    emptyList()
                } else {
                    computeNextStates(state, event)
                }

                Pair(
                    acc.first + nextStates.filter { it.currentState.type != State.Type.FINAL },
                    acc.second + nextStates
                        .filter { it.currentState.type == State.Type.FINAL }
                        .map { extractMatched(it) }
                )
            }

        internalStates = updatedStates + (
            beginningState.getTransitions()
                .filter { it.condition.invoke(event) }
                .map { InternalState(it.to, null, event, timestamp) }
            )

        return results
    }

    private fun computeNextStates(
        internalState: InternalState<T>,
        event: T
    ): Iterable<InternalState<T>> {

        return internalState.currentState.getTransitions().flatMap {
            if (it.condition.invoke(event)) {
                when (it.action) {

                    Transition.Action.TAKE -> {
                        listOf(
                            InternalState(
                                it.to,
                                internalState,
                                event,
                                internalState.startTimestamp
                            )
                        )
                    }

                    Transition.Action.SKIP -> {
                        listOf(internalState)
                    }
                }
            } else {
                emptyList()
            }
        }
    }

    private fun extractMatched(internalState: InternalState<T>): Map<String, Iterable<T>> {
        val results: MutableMap<String, Iterable<T>> = mutableMapOf()
        var currentState: InternalState<T>? = internalState
        do {
            val patternName = currentState!!.currentState.name
            val events = results.getOrElse(patternName) { listOf() }
            results[patternName] = events + currentState.event
            currentState = currentState.parent
        } while (currentState != null)
        return results
    }
}
