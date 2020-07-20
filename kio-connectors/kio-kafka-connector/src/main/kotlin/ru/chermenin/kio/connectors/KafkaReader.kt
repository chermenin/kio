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

package ru.chermenin.kio.connectors

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.kafka.KafkaIO
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import ru.chermenin.kio.io.Reader
import ru.chermenin.kio.utils.Configurable
import ru.chermenin.kio.utils.hashWithName

/**
 * Definition for Kafka reader.
 */
class KafkaReader<K, V>(val pipeline: Pipeline) :
    Configurable<KafkaIO.Read<K, V>, KafkaReader<K, V>>()

/**
 * Method to create Kafka reader.
 */
inline fun <K, V> Reader.kafka(): KafkaReader<K, V> {
    return KafkaReader(pipeline)
}

inline fun <K, V> KafkaReader<K, V>.topic(vararg topic: String): PCollection<KV<K, V>> {
    val reader = getConfigurator().invoke(KafkaIO.read<K, V>().withTopics(topic.toList())).withoutMetadata()
    return this.pipeline.apply(
        reader.hashWithName("Kafka().topic(${topic.joinToString(", ")})"),
        reader
    )
}
