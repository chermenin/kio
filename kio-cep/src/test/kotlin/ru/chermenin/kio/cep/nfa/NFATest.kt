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

import java.util.*
import org.joda.time.Duration
import org.junit.Assert.*
import org.junit.Test
import ru.chermenin.kio.cep.pattern.Pattern

/**
 * Test class to check NFA.
 */
class NFATest {

    @Test
    fun checkSimpleNFA() {

        // create NFA from simple pattern
        val pattern = Pattern
            .startWith<Any>("start")
            .thenFollowByAny("next")
            .then("end")
            .within(Duration.standardSeconds(10))
        val nfa = pattern.compile()

        // collect states from NFA
        val stateStack = Stack<State<Any>>()
        stateStack.push(nfa.beginningState)
        val stateMap = mutableMapOf<String, State<Any>>()
        while (!stateStack.empty()) {
            val state = stateStack.pop()
            stateMap[state.name] = state
            state.getTransitions()
                .filter { it.action != Transition.Action.SKIP }
                .map { it.to }
                .forEach { stateStack.push(it) }
        }

        // check the beginning state
        assertTrue(stateMap.containsKey(Pattern.BEGINNING_STATE_NAME))
        val beginningState = stateMap[Pattern.BEGINNING_STATE_NAME]
        assertTrue(beginningState?.type == State.Type.START)
        val beginningTransitions = beginningState?.getTransitions()?.map { it.to.name to it.action }
        assertEquals(listOf("start" to Transition.Action.TAKE), beginningTransitions)

        // check "start" state
        assertTrue(stateMap.containsKey("start"))
        val startState = stateMap["start"]
        assertTrue(startState?.type == State.Type.INTERMEDIATE)
        val startTransitions = startState?.getTransitions()?.map { it.to.name to it.action }
        assertEquals(listOf(
            "next" to Transition.Action.TAKE,
            "start" to Transition.Action.SKIP
        ), startTransitions)

        // check "next" state
        assertTrue(stateMap.containsKey("next"))
        val nextState = stateMap["next"]
        assertTrue(nextState?.type == State.Type.INTERMEDIATE)
        val nextTransitions = nextState?.getTransitions()?.map { it.to.name to it.action }
        assertEquals(listOf("end" to Transition.Action.TAKE), nextTransitions)

        // check "end" stateR
        assertTrue(stateMap.containsKey("end"))
        val endState = stateMap["end"]
        assertTrue(endState?.type == State.Type.FINAL)
        assertTrue(endState?.getTransitions()?.isEmpty() ?: false)

        // check window
        assertEquals(10L, nfa.window?.standardSeconds)
    }
}
