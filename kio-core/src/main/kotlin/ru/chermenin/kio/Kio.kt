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

package ru.chermenin.kio

import java.lang.IllegalStateException
import java.util.*
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.PipelineResult
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.io.GenerateSequence
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.values.PCollection
import org.joda.time.Duration
import ru.chermenin.kio.arguments.Arguments
import ru.chermenin.kio.coders.DataCoder
import ru.chermenin.kio.coders.PairCoder
import ru.chermenin.kio.io.Reader
import ru.chermenin.kio.options.KioOptions
import ru.chermenin.kio.utils.hashWithName

/**
 * Common context class.
 *
 * @property pipeline current pipeline definition
 * @property arguments additional arguments
 */
class Kio private constructor(private val pipeline: Pipeline, val arguments: Arguments = Arguments(mapOf())) {

    companion object {

        /**
         * Create a context with default settings.
         *
         * @return context object
         */
        fun create(): Kio {
            val (options, _) = Arguments.parse(emptyArray())
            enrichKioOptions(options)
            return Kio(Pipeline.create(options))
        }

        /**
         * Create a context with settings from arguments.
         *
         * @param args arguments array
         * @return context object
         */
        fun fromArguments(args: Array<String>): Kio {
            val (options, arguments) = Arguments.parse(args)
            enrichKioOptions(options)
            return Kio(Pipeline.create(options), arguments)
        }

        /**
         * Create a context with the defined pipeline.
         *
         * @param pipeline original pipeline
         * @return context object
         */
        fun fromPipeline(pipeline: Pipeline): Kio {
            enrichKioOptions(pipeline.options)
            return Kio(pipeline)
        }

        private fun enrichKioOptions(options: PipelineOptions) {
            if (options is KioOptions) {
                val properties = Properties()
                properties.load(this::class.java.classLoader.getResourceAsStream("version.properties"))
                options.kioVersion = properties.getProperty("kio.version")
                options.kotlinVersion = KotlinVersion.CURRENT.toString()
            }
        }
    }

    init {
        FileSystems.setDefaultPipelineOptions(pipeline.options)
        pipeline.coderRegistry.let {
            it.registerCoderProvider(PairCoder.Provider())
            it.registerCoderProvider(DataCoder.Provider())
        }
    }

    /**
     * Current pipeline options.
     */
    val options: PipelineOptions = pipeline.options

    /**
     * Create collection from iterable of some elements.
     *
     * @param elements iterable of elements
     * @param name operator name (optional)
     * @param coder coder for elements (optional)
     * @return collection of the elements
     */
    fun <T> parallelize(elements: Iterable<T>, name: String = "", coder: Coder<T>? = null): PCollection<T> {
        val generator = Create.of(elements).let {
            if (coder != null) it.withCoder(coder) else it
        }
        return pipeline.apply(name.ifEmpty { generator.hashWithName("parallelize") }, generator)
    }

    /**
     * Generate a bounded or unbounded collection of numbers with defined rate.
     *
     * @param from inclusive first element of the sequence (optional, default is 0)
     * @param to exclusive end of the sequence (optional, the collection will be unbounded if it's null)
     * @param rate pair with the number of elements per period and the period for that (optional)
     * @param maxReadTime timeout to finish the collection generating (optional)
     * @param name operator name (optional)
     * @return collection of long values
     */
    fun generate(
        from: Long = 0,
        to: Long? = null,
        rate: Pair<Long, Duration>? = null,
        maxReadTime: Duration? = null,
        name: String = ""
    ): PCollection<Long> {
        val generator = GenerateSequence.from(from).let {
            if (to != null) it.to(to) else it
        }.let {
            if (rate != null) it.withRate(rate.first, rate.second) else it
        }.let {
            if (maxReadTime != null) it.withMaxReadTime(maxReadTime) else it
        }
        return pipeline.apply(name.ifEmpty { generator.hashWithName("generate") }, generator)
    }

    /**
     * Get reader object to get data collections from other sources.
     */
    fun read(): Reader {
        return Reader(pipeline)
    }

    /**
     * Execute the job.
     *
     * @return execution context
     */
    fun execute(): ExecutionContext {
        return ExecutionContext(pipeline.run())
    }

    /**
     * Defines execution context.
     */
    class ExecutionContext(private val pipelineResult: PipelineResult) {

        fun isCompleted(): Boolean = pipelineResult.state.isTerminal

        fun getState(): PipelineResult.State = pipelineResult.state ?: PipelineResult.State.UNKNOWN

        fun waitUntilFinish(duration: Duration = Duration.ZERO): PipelineResult.State {
            try {
                pipelineResult.waitUntilFinish(org.joda.time.Duration(duration.millis))
                return getState()
            } catch (e: InterruptedException) {
                pipelineResult.cancel()
                throw InterruptedException("Job cancelled after exceeding timeout value $duration")
            }
        }

        fun waitUntilDone(duration: Duration = Duration.ZERO) {
            val state = waitUntilFinish(duration)
            if (state != PipelineResult.State.DONE) {
                throw Pipeline.PipelineExecutionException(IllegalStateException("Job finished with state $state"))
            }
        }
    }
}
