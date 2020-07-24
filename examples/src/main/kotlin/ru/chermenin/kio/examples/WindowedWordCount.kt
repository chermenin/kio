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

import java.io.IOException
import java.util.concurrent.ThreadLocalRandom
import org.apache.beam.sdk.io.FileBasedSink
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.io.fs.ResolveOptions
import org.apache.beam.sdk.io.fs.ResourceId
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.IntervalWindow
import org.apache.beam.sdk.transforms.windowing.PaneInfo
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PDone
import org.joda.time.Duration
import org.joda.time.Instant
import org.joda.time.format.ISODateTimeFormat
import ru.chermenin.kio.Kio
import ru.chermenin.kio.functions.map
import ru.chermenin.kio.functions.withFixedWindow

/**
 * An example that counts words in text, and can run over either unbounded or bounded input
 * collections.
 *
 * Note: this and other examples are ported from the Beam's examples written in Kotlin.
 *
 *
 * This class, [WindowedWordCount], is the last in a series of four successively more
 * detailed 'word count' examples. First take a look at [MinimalWordCount], [WordCount],
 * and [DebuggingWordCount].
 *
 *
 * Basic concepts, also in the MinimalWordCount, WordCount, and DebuggingWordCount examples:
 * Reading text files; counting a PCollection; writing to GCS; executing a Pipeline both locally and
 * using a selected runner; defining DoFns; user-defined PTransforms; defining PipelineOptions.
 *
 *
 * New Concepts:
 *
 * <pre>
 * 1. Unbounded and bounded pipeline input modes
 * 2. Adding timestamps to data
 * 3. Windowing
 * 4. Re-using PTransforms over windowed PCollections
 * 5. Accessing the window of an element
 * 6. Writing data to per-window text files
</pre> *
 *
 *
 * By default, the examples will run with the `DirectRunner`. To change the runner,
 * specify:
 *
 * <pre>`--runner=YOUR_SELECTED_RUNNER
`</pre> *
 *
 * See examples/kotlin/README.md for instructions about how to configure different runners.
 *
 *
 * To execute this pipeline locally, specify a local output file (if using the `DirectRunner`) or output prefix on a supported distributed file system.
 *
 * <pre>`--output=[YOUR_LOCAL_FILE | YOUR_OUTPUT_PREFIX]
`</pre> *
 *
 *
 * The input file defaults to a public data set containing the text of of King Lear, by William
 * Shakespeare. You can override it and choose your own input with `--inputFile`.
 *
 *
 * By default, the pipeline will do fixed windowing, on 10-minute windows. You can change this
 * interval by setting the `--windowSize` parameter, e.g. `--windowSize=15` for
 * 15-minute windows.
 *
 *
 * The example will try to cancel the pipeline on the signal to terminate the process (CTRL-C).
 */
object WindowedWordCount {

    /**
     * A DoFn that sets the data element timestamp. This is a silly method,
     * just for this example, for the bounded data case.
     *
     *
     * Imagine that many ghosts of Shakespeare are all typing madly at the same time to recreate
     * his masterworks. Each line of the corpus will get a random associated timestamp somewhere in a
     * 2-hour period.
     */
    class AddTimestampFn(
        private val minTimestamp: Instant,
        private val maxTimestamp: Instant
    ) : DoFn<String, String>() {

        @ProcessElement
        fun processElement(@Element element: String, receiver: OutputReceiver<String>) {

            // Generate timestamp
            val randomTimestamp = Instant(
                ThreadLocalRandom.current()
                    .nextLong(minTimestamp.millis, maxTimestamp.millis)
            )

            // Set the data element with that timestamp.
            receiver.outputWithTimestamp(element, randomTimestamp)
        }
    }

    /**
     * A [DoFn] that writes elements to files with names deterministically derived from the lower
     * and upper bounds of their key (an [IntervalWindow]).
     *
     * This is test utility code, not for end-users, so examples can be focused on their primary lessons.
     */
    class WriteOneFilePerWindow(private val path: String) : PTransform<PCollection<String>, PDone>() {

        companion object {
            private val FORMATTER = ISODateTimeFormat.hourMinute()
        }

        override fun expand(input: PCollection<String>): PDone {
            val resource = FileBasedSink.convertToFileResourceIfPossible(path)
            return input.apply(
                TextIO.write()
                    .to(PerWindowFiles(resource))
                    .withTempDirectory(resource.currentDirectory)
                    .withWindowedWrites()
            )
        }

        /**
         * A [FilenamePolicy] produces a base file name for a write based on metadata about the data
         * being written. This always includes the shard number and the total number of shards. For
         * windowed writes, it also includes the window and pane index (a sequence number assigned to each
         * trigger firing).
         */
        class PerWindowFiles(private val baseFilename: ResourceId) : FilenamePolicy() {

            private fun filenamePrefixForWindow(window: IntervalWindow): String {
                val prefix = if (baseFilename.isDirectory) "" else baseFilename.filename ?: ""
                return "$prefix-${FORMATTER.print(window.start())}-${FORMATTER.print(window.end())}"
            }

            override fun windowedFilename(
                shardNumber: Int,
                numShards: Int,
                window: BoundedWindow,
                paneInfo: PaneInfo,
                outputFileHints: FileBasedSink.OutputFileHints
            ): ResourceId {
                return baseFilename
                    .currentDirectory
                    .resolve(
                        "${filenamePrefixForWindow(window as IntervalWindow)}-$shardNumber-of-" +
                            "$numShards${outputFileHints.suggestedFilenameSuffix}",
                        ResolveOptions.StandardResolveOptions.RESOLVE_FILE
                    )
            }

            override fun unwindowedFilename(
                shardNumber: Int,
                numShards: Int,
                outputFileHints: FileBasedSink.OutputFileHints
            ): ResourceId {
                throw UnsupportedOperationException()
            }
        }
    }

    @Throws(IOException::class)
    @JvmStatic
    fun main(args: Array<String>) {

        // Create the Kio context using arguments
        val kio = Kio.fromArguments(args)

        // Get options from arguments
        val inputFile = kio.arguments.getOrElse("input") { "gs://apache-beam-samples/shakespeare/kinglear.txt" }
        val output = kio.arguments.required("output")

        val windowSize = kio.arguments.long("windowSize") { 10L } // default: 10 minutes

        val minTimestamp = Instant(kio.arguments.long("minTimestampMillis") { System.currentTimeMillis() })
        val maxTimestamp = Instant(kio.arguments.long("maxTimestampMillis") {
            minTimestamp.plus(Duration.standardHours(1)).millis
        })

        // The Beam SDK lets us run the same pipeline with either a bounded or unbounded input source.
        kio.read().text(inputFile)

            // Add an element timestamp, using an artificial time just to show windowing.
            // See AddTimestampFn for more detail on this.
            .apply(ParDo.of(AddTimestampFn(minTimestamp, maxTimestamp)))

            // Window into fixed windows. The fixed window size for this example defaults to 1 minute
            // (you can change this with a command-line option). See the documentation for more
            // information on how fixed windows work, and for information on the other types of windowing
            // available (e.g., sliding windows).
            .withFixedWindow(Duration.standardMinutes(windowSize))

            // Re-use our existing CountWords transform that does not have knowledge of
            // windows over a PCollection containing windowed values.
            .apply(WordCount.CountWords())

            // Format the results and write to a sharded file partitioned by window
            .map { "${it.key}: ${it.value}" }
            .apply(WriteOneFilePerWindow(output))

        kio.execute().waitUntilDone()
    }
}
