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

import com.google.api.services.bigquery.model.*
import com.twitter.chill.ClosureCleaner
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.ValueInSingleWindow
import ru.chermenin.kio.io.Writer
import ru.chermenin.kio.utils.hashWithName

/**
 * Definition for BigQuery writer.
 */
class BigQueryWriter<T>(val collection: PCollection<T>)

/**
 * Method to create BigQuery writer.
 */
inline fun <T> Writer<T>.bigQuery(): BigQueryWriter<T> {
    return BigQueryWriter(collection)
}

/**
 * Write data to dynamic destinations.
 *
 * @param destinations definition of dynamic destinations
 */
inline fun <T> BigQueryWriter<T>.dynamicDestinations(
    destinations: DynamicDestinations<T, *>,
    build: BigQueryIO.Write<T>.() -> BigQueryIO.Write<T> = { this }
) {
    val writer = BigQueryIO.write<T>().build().to(destinations)
    collection.apply(writer.hashWithName("BigQuery().dynamicDestinations($destinations)"), writer)
}

/**
 * Write data to dynamic destinations.
 *
 * @param getTable function to get table from the element
 * @param getSchema function to get schema from the element
 */
inline fun <T> BigQueryWriter<T>.dynamicDestinations(
    noinline getTable: (T) -> TableDestination,
    noinline getSchema: (T) -> TableSchema,
    build: BigQueryIO.Write<T>.() -> BigQueryIO.Write<T> = { this }
) {
    this.dynamicDestinations(object : DynamicDestinations<T, Pair<TableDestination, TableSchema>>() {
        private val t = ClosureCleaner.clean(getTable)
        private val s = ClosureCleaner.clean(getSchema)

        override fun getDestination(element: ValueInSingleWindow<T>): Pair<TableDestination, TableSchema> {
            return Pair(t(element.value!!), s(element.value!!))
        }

        override fun getTable(destination: Pair<TableDestination, TableSchema>): TableDestination {
            return destination.first
        }

        override fun getSchema(destination: Pair<TableDestination, TableSchema>): TableSchema {
            return destination.second
        }
    }, build)
}

/**
 * Write rows into the table.
 *
 * @param tableSpec a spec of the destination table
 */
@JvmName("writeTableRow")
inline fun BigQueryWriter<TableRow>.table(
    tableSpec: String,
    build: BigQueryIO.Write<TableRow>.() -> BigQueryIO.Write<TableRow> = { this }
) {
    val writer = BigQueryIO.writeTableRows().build().to(tableSpec)
    collection.apply(writer.hashWithName("BigQuery().table($tableSpec)"), writer)
}

/**
 * Write rows into the table.
 *
 * @param tableRef a reference to the destination table
 */
@JvmName("writeTableRow")
inline fun BigQueryWriter<TableRow>.table(
    tableRef: TableReference,
    build: BigQueryIO.Write<TableRow>.() -> BigQueryIO.Write<TableRow> = { this }
) {
    val writer = BigQueryIO.writeTableRows().build().to(tableRef)
    collection.apply(writer.hashWithName("BigQuery().table($tableRef)"), writer)
}

/**
 * Write data to the table.
 *
 * @param tableSpec a spec of the destination table
 */
inline fun <T> BigQueryWriter<T>.table(
    tableSpec: String,
    build: BigQueryIO.Write<T>.() -> BigQueryIO.Write<T> = { this }
) {
    val writer = BigQueryIO.write<T>().build().to(tableSpec)
    collection.apply(writer.hashWithName("BigQuery().table($tableSpec)"), writer)
}

/**
 * Write data into the table.
 *
 * @param tableRef a reference to the destination table
 */
inline fun <T> BigQueryWriter<T>.table(
    tableRef: TableReference,
    build: BigQueryIO.Write<T>.() -> BigQueryIO.Write<T> = { this }
) {
    val writer = BigQueryIO.write<T>().build().to(tableRef)
    collection.apply(writer.hashWithName("BigQuery().table($tableRef)"), writer)
}
