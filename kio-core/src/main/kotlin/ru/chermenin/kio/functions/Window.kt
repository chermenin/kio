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

import org.apache.beam.sdk.transforms.Wait
import org.apache.beam.sdk.transforms.windowing.*
import org.apache.beam.sdk.values.*
import org.joda.time.Duration
import ru.chermenin.kio.utils.Configurator
import ru.chermenin.kio.utils.hashWithName

/**
 * Method to window collection by defined function.
 *
 * @param windowFn window function
 * @param options additional windowing options (optional)
 * @return windowed collection
 */
inline fun <T> PCollection<T>.withWindow(
    windowFn: WindowFn<T, *>,
    options: Configurator<Window<T>> = { it }
): PCollection<T> {
    val window = options.invoke(Window.into(windowFn))
    return this.apply(window.hashWithName("withWindow"), window)
}

@Suppress("UNCHECKED_CAST")
inline fun <T> PCollection<T>.withWindowByDays(
    number: Int,
    options: Configurator<Window<T>> = { it }
): PCollection<T> {
    val windowFn = CalendarWindows.days(number) as WindowFn<T, *>
    return this.withWindow(windowFn, options)
        .setName(windowFn.hashWithName("withCalendarWindowByDays($number)"))
}

@Suppress("UNCHECKED_CAST")
inline fun <T> PCollection<T>.withWindowByWeeks(
    number: Int,
    startDayOfWeek: Int,
    options: Configurator<Window<T>> = { it }
): PCollection<T> {
    val windowFn = CalendarWindows.weeks(number, startDayOfWeek) as WindowFn<T, *>
    return this.withWindow(windowFn, options)
        .setName(windowFn.hashWithName("withCalendarWindowByWeeks($number, $startDayOfWeek)"))
}

@Suppress("UNCHECKED_CAST")
inline fun <T> PCollection<T>.withWindowByMonths(
    number: Int,
    options: Configurator<Window<T>> = { it }
): PCollection<T> {
    val windowFn = CalendarWindows.months(number) as WindowFn<T, *>
    return this.withWindow(windowFn, options)
        .setName(windowFn.hashWithName("withCalendarWindowByMonths($number)"))
}

@Suppress("UNCHECKED_CAST")
inline fun <T> PCollection<T>.withWindowByYears(
    number: Int,
    options: Configurator<Window<T>> = { it }
): PCollection<T> {
    val windowFn = CalendarWindows.years(number) as WindowFn<T, *>
    return this.withWindow(windowFn, options)
        .setName(windowFn.hashWithName("withCalendarWindowByYears($number)"))
}

@Suppress("UNCHECKED_CAST")
inline fun <T> PCollection<T>.withFixedWindow(
    duration: Duration,
    offset: Duration = Duration.ZERO,
    options: Configurator<Window<T>> = { it }
): PCollection<T> {
    val windowFn = FixedWindows
        .of(duration)
        .withOffset(offset)
        as WindowFn<T, *>
    return this.withWindow(windowFn, options)
        .setName(windowFn.hashWithName("withFixedWindow($duration, $offset)"))
}

@Suppress("UNCHECKED_CAST")
inline fun <T> PCollection<T>.withGlobalWindow(
    options: Configurator<Window<T>> = { it }
): PCollection<T> {
    val windowFn = GlobalWindows() as WindowFn<T, *>
    return this.withWindow(windowFn, options)
        .setName(windowFn.hashWithName("withGlobalWindow()"))
}

@Suppress("UNCHECKED_CAST")
inline fun <T> PCollection<T>.withSessionWindow(
    gap: Duration,
    options: Configurator<Window<T>> = { it }
): PCollection<T> {
    val windowFn = Sessions
        .withGapDuration(gap)
        as WindowFn<T, *>
    return this.withWindow(windowFn, options)
        .setName(windowFn.hashWithName("withSessionWindows($gap)"))
}

@Suppress("UNCHECKED_CAST")
inline fun <T> PCollection<T>.withSlidingWindow(
    size: Duration,
    period: Duration,
    offset: Duration = Duration.ZERO,
    options: Configurator<Window<T>> = { it }
): PCollection<T> {
    val windowFn = SlidingWindows
        .of(size)
        .every(period)
        .withOffset(offset)
        as WindowFn<T, *>
    return this.withWindow(windowFn, options)
        .setName(windowFn.hashWithName("withSlidingWindow($size, $period, $offset)"))
}

inline fun <T> PCollection<T>.wait(vararg signals: PCollection<*>): PCollection<T> {
    val waiter = Wait.on<T>(*signals)
    return this.apply(waiter.hashWithName("wait"), waiter)
}
