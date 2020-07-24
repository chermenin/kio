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

package ru.chermenin.kio.examples

import ru.chermenin.kio.Kio
import ru.chermenin.kio.functions.*
import ru.chermenin.kio.io.text
import ru.chermenin.kio.io.write

/**
 * An example that counts words in Shakespeare.
 *
 * Note: this and other examples are ported from the Beam's examples written in Kotlin.
 *
 *
 * This class, [MinimalWordCount], is the first in a series of four successively more
 * detailed 'word count' examples. Here, for simplicity, we don't show any error-checking or
 * argument processing, and focus on construction of the pipeline, which chains together the
 * application of core transforms.
 *
 *
 * Next, see the [WordCount] pipeline, then the [DebuggingWordCount], and finally the
 * [WindowedWordCount] pipeline, for more detailed examples that introduce additional
 * concepts.
 *
 *
 * Concepts:
 * 1. Reading data from text files
 * 2. Specifying transform functions
 * 3. Counting items in a PCollection
 * 4. Writing data to text files
 *
 *
 * No arguments are required to run this pipeline. It will be executed with the DirectRunner. You
 * can see the results in the output files in your current working directory, with names like
 * `wordcounts-00001-of-00005`.
 */
object MinimalWordCount {

    @JvmStatic
    fun main(args: Array<String>) {

        // Create the Kio context. The context lets set various execution options for our pipeline,
        // such as the runner you wish to use. This example will run with the DirectRunner by default,
        // based on the class path configured in its dependencies.
        val kio = Kio.create()

        // This example reads a public data set consisting of the complete works of Shakespeare.
        kio.read().text("gs://apache-beam-samples/shakespeare/*")

            // Apply a flatmap function to the PCollection of text lines.
            // This transform splits the lines in PCollection<String>, where each element is an
            // individual word in Shakespeare's collected texts.
            .flatMap { it.split(WordCount.TOKENIZER_PATTERN) }

            // We use a Filter transform to avoid empty word
            .filter { it.isNotBlank() }

            // Apply the count by values function to our PCollection of individual words.
            // It returns a new PCollection of key/value pairs, where each key represents a
            // unique word in the text. The associated value is the occurrence count for that word.
            .countByValue()

            // Apply a map function that formats our PCollection of word counts into a
            // printable string, suitable for writing to an output file.
            .map { "${it.key}: ${it.value}" }

            // Apply a write function at the end of the pipeline.
            // It writes the contents of a PCollection (in this case, our PCollection of
            // formatted strings) to a series of text files.
            //
            // By default, it will write to a set of files with names like `wordcounts-00001-of-00005`.
            .write().text("wordcounts")

        kio.execute().waitUntilDone()
    }
}
