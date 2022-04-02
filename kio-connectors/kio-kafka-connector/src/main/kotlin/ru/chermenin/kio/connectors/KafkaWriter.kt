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

import org.apache.beam.sdk.io.kafka.KafkaIO
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import ru.chermenin.kio.io.Writer
import ru.chermenin.kio.utils.hashWithName

/**
 * Definition for Kafka writer.
 */
class KafkaWriter<K, V>(val collection: PCollection<KV<K, V>>)

/**
 * Method to create Kafka writer.
 */
inline fun <K, V> Writer<KV<K, V>>.kafka(): KafkaWriter<K, V> {
    return KafkaWriter(collection)
}

inline fun <K, V> KafkaWriter<K, V>.topic(
    topic: String,
    build: KafkaIO.Write<K, V>.() -> KafkaIO.Write<K, V> = { this }
) {
    val writer = KafkaIO.write<K, V>().build().withTopic(topic)
    collection.apply(writer.hashWithName("Kafka().topic($topic)"), writer)
}
