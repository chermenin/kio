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

package ru.chermenin.kio.cep.pattern

import java.io.Serializable
import org.joda.time.Duration
import ru.chermenin.kio.cep.nfa.NFA
import ru.chermenin.kio.cep.nfa.State

/**
 * Class to define a pattern to match elements.
 *
 * @property name current pattern name
 * @property parent previous pattern in the chain
 * @property consuming type of consuming
 * @property condition condition for the current pattern
 * @param T events type
 */
abstract class Pattern<T>(
    val name: String,
    val parent: Pattern<T>?,
    val consuming: Consuming,
    val condition: (T) -> Boolean
) : Serializable {

    companion object {

        internal const val BEGINNING_STATE_NAME: String = "$\$beginningState$$"

        /**
         * Start a new pattern with provided name and condition.
         *
         * @param name pattern name
         * @param condition pattern condition (optional)
         * @param T event type
         */
        fun <T> startWith(name: String, condition: (T) -> Boolean = { true }): PatternBuilder<T> {
            return PatternBuilder(name, condition = condition)
        }
    }

    enum class Consuming {
        NEXT,
        FOLLOW_BY,
        FOLLOW_BY_ANY
    }

    /**
     * Class to build the chain of patterns.
     *
     * @property name current pattern name
     * @property parent previous pattern in the chain
     * @property consuming type of consuming
     * @property condition condition for the current pattern
     */
    class PatternBuilder<T>(
        private val name: String,
        private val parent: PatternBuilder<T>? = null,
        private val consuming: Consuming = Consuming.NEXT,
        private val condition: (T) -> Boolean
    ) {

        /**
         * Add a pattern element to the chain. This one only matches if an event which matches
         * this pattern element directly follows the previous matching event.
         *
         * @param name current pattern element name
         * @param condition condition for current pattern element (optional)
         * @return pattern builder
         */
        fun then(name: String, condition: (T) -> Boolean = { true }): PatternBuilder<T> {
            return PatternBuilder(name, this, condition = condition)
        }

        /**
         * Add a pattern element to the chain. In this case between the current matching event
         * and the previous one can be placed other events (not matched to condition only)
         * which will be ignored.
         *
         * @param name current pattern element name
         * @param condition condition for current pattern element (optional)
         * @return pattern builder
         */
        fun thenFollowBy(name: String, condition: (T) -> Boolean = { true }): PatternBuilder<T> {
            return PatternBuilder(name, this, Consuming.FOLLOW_BY, condition)
        }

        /**
         * Add a pattern element to the chain. In this case between the current matching event
         * and the previous one can be placed other events which will be ignored.
         *
         * @param name current pattern element name
         * @param condition condition for current pattern element (optional)
         * @return pattern builder
         */
        fun thenFollowByAny(name: String, condition: (T) -> Boolean = { true }): PatternBuilder<T> {
            return PatternBuilder(name, this, Consuming.FOLLOW_BY_ANY, condition)
        }

        /**
         * Create a pattern with defined maximum time duration to match the pattern.
         * Note: This pattern can be used with GlobalWindow only.
         *
         * @param duration max window length
         * @return pattern
         */
        fun within(duration: Duration): Pattern<T> {
            return TimePattern(
                this.name,
                this.parent?.within(Duration.ZERO),
                consuming,
                condition,
                duration
            )
        }

        /**
         * Create a pattern without a built-in window time size.
         * Note: This pattern can not be used with GlobalWindow.
         *
         * @return pattern
         */
        fun withinWindow(): Pattern<T> {
            return WindowPattern(
                this.name,
                this.parent?.withinWindow(),
                consuming,
                condition
            )
        }
    }

    internal fun compile(): NFA<T> {
        val states = mutableMapOf<String, State<T>>()

        var currentPattern: Pattern<T> = this
        var nextPattern: Pattern<T>
        var nextState: State<T>

        var currentState: State<T> = State(currentPattern.name, State.Type.FINAL)
        states[currentState.name] = currentState

        while (currentPattern.parent != null) {
            nextPattern = currentPattern
            nextState = currentState
            currentPattern = currentPattern.parent!!
            if (states.containsKey(currentPattern.name)) {
                currentState = states[currentPattern.name]!!
            } else {
                currentState = State(currentPattern.name, State.Type.INTERMEDIATE)
                states[currentState.name] = currentState
            }
            currentState.take(nextPattern.condition, nextState)
            when (nextPattern.consuming) {
                Consuming.FOLLOW_BY -> currentState.skip({ !nextPattern.condition(it) }, currentState)
                Consuming.FOLLOW_BY_ANY -> currentState.skip({ true }, currentState)
                else -> { }
            }
        }

        val beginningState: State<T> = State(BEGINNING_STATE_NAME, State.Type.START)
        beginningState.take(currentPattern.condition, currentState)
        return NFA(beginningState, if (this is TimePattern) this.window else null)
    }
}
