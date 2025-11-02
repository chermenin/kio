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

package ru.chermenin.kio.arguments

import java.beans.Introspector
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import ru.chermenin.kio.options.KioOptions

class Arguments internal constructor(private val argumentsMap: Map<String, List<String>>) {

    companion object {

        // Used to trim application arguments string for UI if it's too long
        private const val ARGUMENTS_STRING_MAX_LENGTH = 49152

        fun parse(args: Array<String>): Pair<PipelineOptions, Arguments> {
            val classes = PipelineOptionsFactory.getRegisteredOptions() + KioOptions::class.java

            val optPatterns = classes.flatMap { clazz ->
                clazz.methods
                    .flatMap {
                        val n = it.name
                        if ((!n.startsWith("get") && !n.startsWith("is")) ||
                            it.parameterTypes.isNotEmpty() || it.returnType == Unit::class.java) {
                            emptyList()
                        } else {
                            listOf(Introspector.decapitalize(n.substring(if (n.startsWith("is")) 2 else 3)))
                        }
                    }.map { "--$it(\$|=)".toRegex() }
            }

            // Split arguments - one part for PipelineOptions and another for Arguments
            val (appArgs, optArgs) = args.partition { arg -> optPatterns.none { it.containsMatchIn(arg) } }

            // Create pipeline options
            val pipelineOptions = PipelineOptionsFactory
                .fromArgs(*optArgs.toTypedArray())
                .withValidation()
                .`as`(KioOptions::class.java)

            // Parse rest of arguments
            val (propertiesArgs, booleansArgs) = appArgs.map {
                if (!it.startsWith("--")) {
                    throw IllegalArgumentException("Argument '$it' does not begin with '--'")
                }
                it.trimStart('-')
            }.partition { it.contains('=') }

            val propertiesMap = propertiesArgs
                .map {
                    val (k, v) = it.split("=", limit = 2)
                    Pair(k, v.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)".toRegex()))
                }
                .groupBy { it.first }
                .mapValues { entry -> entry.value.flatMap { it.second } }

            val booleanMap = booleansArgs.map { Pair(it, listOf("true")) }.toMap()

            propertiesMap.keys.intersect(booleanMap.keys).forEach {
                throw IllegalArgumentException("Conflicting arguments with name '$it'")
            }

            val arguments = Arguments(propertiesMap + booleanMap)

            if (appArgs.isNotEmpty()) {
                val argumentsString = arguments.joinToString()
                val safeArgumentsString = if (argumentsString.length > ARGUMENTS_STRING_MAX_LENGTH) {
                    argumentsString.substring(0, ARGUMENTS_STRING_MAX_LENGTH) + " [...]"
                } else {
                    argumentsString
                }

                pipelineOptions.arguments = safeArgumentsString
            }

            return Pair(pipelineOptions, arguments)
        }
    }

    private fun value(key: String): List<String> {
        return argumentsMap.getOrDefault(key, emptyList())
    }

    private fun <T> getAs(key: String, f: (String) -> T): T {
        val value = required(key)
        try {
            return f(value)
        } catch (e: Throwable) {
            throw IllegalArgumentException("Invalid value '$value' for argument with name '$key'")
        }
    }

    private fun <T> getAs(key: String, default: () -> T, f: (String) -> T): T {
        val value = optional(key)
        try {
            return if (value != null) {
                f(value)
            } else {
                default()
            }
        } catch (e: Throwable) {
            throw IllegalArgumentException("Invalid value '$value' for argument with name '$key'")
        }
    }

    fun optional(key: String): String? {
        val values = value(key)
        return when (values.size) {
            0 -> null
            1 -> values[0]
            else -> throw IllegalArgumentException("There are multiple values for argument with name '$key'")
        }
    }

    fun required(key: String): String {
        val values = value(key)
        return when (values.size) {
            0 -> throw IllegalArgumentException("Missed value for argument with name '$key'")
            1 -> values[0]
            else -> throw IllegalArgumentException("There are multiple values for argument with name '$key'")
        }
    }

    operator fun get(key: String): String = required(key)

    fun getOrElse(key: String, default: () -> String): String {
        return optional(key) ?: default()
    }

    fun int(key: String): Int = getAs(key) { it.toInt() }

    fun int(key: String, default: () -> Int): Int = getAs(key, default) { it.toInt() }

    fun long(key: String): Long = getAs(key) { it.toLong() }

    fun long(key: String, default: () -> Long): Long = getAs(key, default) { it.toLong() }

    fun float(key: String): Float = getAs(key) { it.toFloat() }

    fun float(key: String, default: () -> Float): Float = getAs(key, default) { it.toFloat() }

    fun double(key: String): Double = getAs(key) { it.toDouble() }

    fun double(key: String, default: () -> Double): Double = getAs(key, default) { it.toDouble() }

    fun bool(key: String): Boolean = getAs(key, { false }, { it.toBoolean() })

    fun bool(key: String, default: () -> Boolean): Boolean = getAs(key, default) { it.toBoolean() }

    fun joinToString(separator: String = ", ", prefix: String = "", suffix: String = ""): String {
        return argumentsMap.keys.toTypedArray().sortedArray()
            .map {
                val values = when (argumentsMap[it]?.size) {
                    1 -> argumentsMap[it]?.get(0).toString()
                    else -> argumentsMap[it]?.joinToString(", ", "[", "]")
                }
                return@map "--$it=$values"
            }
            .joinToString(separator, prefix, suffix)
    }
}
