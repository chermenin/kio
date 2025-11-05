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

import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import ru.chermenin.kio.Kio
import ru.chermenin.kio.functions.*
import ru.chermenin.kio.io.text
import ru.chermenin.kio.io.write

/**
 * An example that counts words in Shakespeare or any other input file.
 *
 * Note: this and other examples are ported from the Beam's examples written in Kotlin.
 *
 *
 * This class, [WordCount], is the second in a series of four successively more detailed
 * 'word count' examples. You may first want to take a look at [MinimalWordCount]. After
 * you've looked at this example, then see the [DebuggingWordCount] pipeline, for introduction
 * of additional concepts.
 *
 *
 * For a detailed walkthrough of this example, see
 * [https://beam.apache.org/get-started/wordcount-example/](https://beam.apache.org/get-started/wordcount-example/)
 *
 *
 * Basic concepts, also in the MinimalWordCount example:
 * - reading text files;
 * - counting a PCollection;
 * - writing to text files.
 *
 *
 * New Concepts:
 * 1. Executing a Pipeline both locally and using the selected runner
 * 2. Using ParDo with static DoFns defined out-of-line
 * 3. Building a composite transform
 * 4. Get pipeline options from the program arguments
 *
 *
 * You can execute this pipeline either locally or using by selecting another runner. These are
 * now command-line options and not hard-coded as they were in the MinimalWordCount example.
 *
 *
 * To change the runner, specify: `--runner=YOUR_SELECTED_RUNNER`
 *
 *
 * To execute this pipeline, specify a local output file (if using the `DirectRunner`) or
 * output prefix on a supported distributed file system.
 *
 * `--output=[YOUR_LOCAL_FILE | YOUR_OUTPUT_PREFIX]`
 *
 *
 * The input file defaults to a public data set containing the text of of King Lear, by William
 * Shakespeare. You can override it and choose your own input with `--input`.
 */
object WordCount {

    val TOKENIZER_PATTERN = "[^\\p{L}]+".toRegex()

    /**
     * Custom DoFn to split input lines to separate words.
     */
    class ExtractWordsFn : DoFn<String, String>() {
        private val emptyLinesCounter = Metrics.counter(ExtractWordsFn::class.java, "emptyLines")
        private val lineLengthDistribution = Metrics.distribution(ExtractWordsFn::class.java, "lineLength")

        @ProcessElement
        fun processElement(@Element element: String, receiver: OutputReceiver<String>) {
            lineLengthDistribution.update(element.length.toLong())
            if (element.isBlank()) {
                emptyLinesCounter.inc()
            }

            // Split the line into words and send each word encountered into the output PCollection.
            element
                .split(TOKENIZER_PATTERN).toTypedArray()
                .filter { it.isNotBlank() }
                .forEach { receiver.output(it) }
        }
    }

    /**
     * A PTransform that converts a PCollection containing lines of text into a PCollection of
     * formatted word counts.
     *
     * This is a custom composite transform that bundles two transforms (ParDo and count function)
     * as a reusable PTransform subclass. Using composite transforms allows for easy reuse,
     * modular testing, and an improved monitoring experience.
     */
    class CountWords : PTransform<PCollection<String>, PCollection<KV<String, Long>>>() {

        override fun expand(lines: PCollection<String>): PCollection<KV<String, Long>> {

            // Convert lines of text into individual words.
            val words = lines.apply(ParDo.of(ExtractWordsFn()))

            // Count the number of times each word occurs.
            return words.countByValue()
        }
    }

    @JvmStatic
    fun main(args: Array<String>) {

        // Create the Kio context using arguments
        val kio = Kio.fromArguments(args)

        // Configure pipeline using custom `ExtractWordsFn` and built-in functions
        kio.read()
            .text(kio.arguments.getOrElse("input") { "gs://apache-beam-samples/shakespeare/kinglear.txt" })
            .apply(ParDo.of(ExtractWordsFn()))
            .countByValue()
            .map { "${it.key}: ${it.value}" }
            .write().text(kio.arguments.required("output"))

        // Execute the pipeline
        kio.execute().waitUntilDone()
    }
}
