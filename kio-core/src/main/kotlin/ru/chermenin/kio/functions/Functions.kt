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

package ru.chermenin.kio.functions

import java.io.Serializable

interface KioFunction : Serializable

fun interface KioFunction0<T> : KioFunction {
    fun invoke(t: T)
}

fun interface KioFunction1<T1, R> : KioFunction {
    fun invoke(t1: T1): R
}

fun interface KioFunction2<T1, T2, R> : KioFunction {
    fun invoke(t1: T1, t2: T2): R
}
