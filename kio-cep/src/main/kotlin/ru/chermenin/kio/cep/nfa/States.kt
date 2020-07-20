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

package ru.chermenin.kio.cep.nfa

import java.io.Serializable

/**
 * Class to define a state of the NFA with the name and the transitions list.
 *
 * @param name state name
 * @param type state type (see: [State.Type])
 * @param T event type
 */
internal class State<T>(val name: String, val type: Type) : Serializable {

    private val transitions = mutableListOf<Transition<T>>()

    private fun addTransition(action: Transition.Action, to: State<T>, condition: (T) -> Boolean) {
        transitions.add(Transition(to, action, condition))
    }

    fun getTransitions(): List<Transition<T>> {
        return transitions.toList()
    }

    fun take(condition: (T) -> Boolean, to: State<T> = this) {
        addTransition(Transition.Action.TAKE, to, condition)
    }

    fun skip(condition: (T) -> Boolean, to: State<T> = this) {
        addTransition(Transition.Action.SKIP, to, condition)
    }

    enum class Type {
        START,
        FINAL,
        INTERMEDIATE
    }
}

/**
 * Class to define transition from one state to another.
 *
 * @param to destination state
 * @param action action to be taken during the transition
 * @param condition determine the need for transition
 * @param T event type
 */
internal class Transition<T>(
    val to: State<T>,
    val action: Action,
    val condition: (T) -> Boolean
) : Serializable {

    enum class Action {
        TAKE,
        SKIP
    }
}

/**
 * Class to represent an internal state of the NFA.
 *
 * @param currentState current state
 * @param parent previous internal state
 * @param event current event
 * @param startTimestamp timestamp from the beginning state
 * @param T event type
 */
internal class InternalState<T>(
    val currentState: State<T>,
    val parent: InternalState<T>?,
    val event: T,
    val startTimestamp: Long
) : Serializable
