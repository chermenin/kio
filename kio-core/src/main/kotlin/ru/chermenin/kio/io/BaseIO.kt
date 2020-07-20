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

package ru.chermenin.kio.io

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.Compression
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.values.PCollection
import ru.chermenin.kio.utils.hashWithName

/**
 * Class to provide method to read data from somewhere.
 */
inline class Reader(val pipeline: Pipeline) {

    fun text(path: String, compression: Compression = Compression.AUTO): PCollection<String> {
        val textIO = TextIO.read().withCompression(compression).from(path)
        return pipeline.apply(textIO.hashWithName("text"), textIO)
    }

    fun text(name: String, path: String, compression: Compression = Compression.AUTO): PCollection<String> {
        val textIO = TextIO.read().withCompression(compression).from(path)
        return pipeline.apply(name, textIO)
    }
}

/**
 * Class to provide methods to write data to somewhere.
 */
inline class Writer<T>(val collection: PCollection<T>)

fun <T> PCollection<T>.write(): Writer<T> {
    return Writer(this)
}

inline fun Writer<String>.text(
    path: String,
    numShards: Int = 0,
    suffix: String = ".txt",
    compression: Compression = Compression.UNCOMPRESSED
) {
    val textIO = TextIO.write()
        .to(path)
        .withSuffix(suffix)
        .withNumShards(numShards)
        .withCompression(compression)
    this.collection.apply(textIO.hashWithName("text"), textIO)
}

inline fun Writer<String>.text(
    name: String,
    path: String,
    numShards: Int = 0,
    suffix: String = ".txt",
    compression: Compression = Compression.UNCOMPRESSED
) {
    val textIO = TextIO.write()
        .to(path)
        .withSuffix(suffix)
        .withNumShards(numShards)
        .withCompression(compression)
    this.collection.apply(name, textIO)
}
