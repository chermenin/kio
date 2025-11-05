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

package ru.chermenin.kio.examples

import java.util.regex.Pattern
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.KV
import org.slf4j.LoggerFactory
import ru.chermenin.kio.Kio
import ru.chermenin.kio.test.that

/**
 * An example that verifies word counts in Shakespeare.
 *
 * Note: this and other examples are ported from the Beam's examples written in Kotlin.
 *
 *
 * This class, [DebuggingWordCount], is the third in a series of four successively more
 * detailed 'word count' examples. You may first want to take a look at [MinimalWordCount] and
 * [WordCount]. After you've looked at this example, then see the [WindowedWordCount]
 * pipeline, for introduction of additional concepts.
 *
 *
 * Basic concepts, also in the MinimalWordCount and WordCount examples:
 * - reading text files;
 * - counting a PCollection;
 * - executing a Pipeline both locally and using a selected runner;
 * - defining DoFns.
 *
 *
 * New Concepts:
 * 1. Logging using SLF4J, even in a distributed environment
 * 2. Creating a custom metric (runners have varying levels of support)
 * 3. Testing your pipeline
 *
 *
 * To change the runner, specify: `--runner=YOUR_SELECTED_RUNNER`.
 *
 *
 * The input file defaults to a public data set containing the text of of King Lear, by William
 * Shakespeare. You can override it and choose your own input with `--input`.
 */
object DebuggingWordCount {

    /**
     * A DoFn that filters for a specific key based upon a regular expression.
     */
    class FilterTextFn(pattern: String) : DoFn<KV<String, Long>, KV<String, Long>>() {

        companion object {

            /**
             * The logger below uses the fully qualified class name of FilterTextFn as the logger.
             * Depending on your SLF4J configuration, log statements will likely be qualified by this name.
             *
             * Note that this is entirely standard SLF4J usage. Some runners may provide a default SLF4J
             * configuration that is most appropriate for their logging integration.
             */
            private val logger = LoggerFactory.getLogger(FilterTextFn::class.java)
        }

        private val filter: Pattern = Pattern.compile(pattern)

        /**
         * A custom metric can track values in your pipeline as it runs. Each runner provides
         * varying levels of support for metrics, and may expose them in a dashboard, etc.
         */
        private val matchedWords = Metrics.counter(FilterTextFn::class.java, "matchedWords")
        private val unmatchedWords = Metrics.counter(FilterTextFn::class.java, "unmatchedWords")

        @ProcessElement
        fun processElement(context: ProcessContext) {
            val element = context.element()
            if (filter.matcher(element.key!!).matches()) {

                // Log at the "DEBUG" level each element that we match. When executing this pipeline
                // these log lines will appear only if the log level is set to "DEBUG" or lower.
                logger.debug("Matched: ${element.key}")
                matchedWords.inc()
                context.output(context.element())
            } else {

                // Log at the "TRACE" level each element that is not matched. Different log levels
                // can be used to control the verbosity of logging providing an effective mechanism
                // to filter less important information.
                logger.trace("Did not match: ${element.key}")
                unmatchedWords.inc()
            }
        }
    }

    @JvmStatic
    fun main(args: Array<String>) {

        // Create the Kio context using arguments
        val kio = Kio.fromArguments(args)

        val input = kio.arguments.getOrElse("input") { "gs://apache-beam-samples/shakespeare/kinglear.txt" }

        val filteredWords = kio.read().text(input)
            .apply(WordCount.CountWords())
            .apply(ParDo.of(FilterTextFn(kio.arguments.getOrElse("filterPattern") { "Flourish|stomach" })))

        filteredWords.that().containsInAnyOrder(
            KV.of("Flourish", 3L),
            KV.of("stomach", 1L)
        )

        kio.execute().waitUntilDone()
    }
}
