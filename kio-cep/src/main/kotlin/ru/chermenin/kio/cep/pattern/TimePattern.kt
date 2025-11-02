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

package ru.chermenin.kio.cep.pattern

import org.joda.time.Duration
import ru.chermenin.kio.functions.KioFunction1

/**
 * Represents timed pattern.

 * @property name current pattern name
 * @property parent previous pattern in the chain
 * @property consuming type of consuming
 * @property condition condition for the current pattern
 * @property window time window length
 * @param T events type
 */
internal class TimePattern<T>(
    name: String,
    parent: Pattern<T>?,
    consuming: Consuming,
    condition: KioFunction1<T, Boolean>,
    val window: Duration
) : Pattern<T>(name, parent, consuming, condition)
