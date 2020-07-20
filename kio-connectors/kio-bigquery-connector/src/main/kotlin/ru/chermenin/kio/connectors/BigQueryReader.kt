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

import com.google.api.services.bigquery.model.TableReference
import com.google.api.services.bigquery.model.TableRow
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.values.PCollection
import ru.chermenin.kio.io.Reader
import ru.chermenin.kio.utils.Configurable
import ru.chermenin.kio.utils.hashWithName

/**
 * Definition for BigQuery reader.
 */
class BigQueryReader(val pipeline: Pipeline) :
    Configurable<BigQueryIO.TypedRead<TableRow>, BigQueryReader>()

/**
 * Method to create BigQuery reader.
 */
inline fun Reader.bigQuery(): BigQueryReader {
    return BigQueryReader(pipeline)
}

/**
 * Read data by select.
 *
 * @param query query to select
 * @return collection of table rows
 */
inline fun BigQueryReader.select(query: String): PCollection<TableRow> {
    val reader = getConfigurator().invoke(BigQueryIO.readTableRows().fromQuery(query))
    return pipeline.apply(reader.hashWithName("BigQuery().select($query)"), reader)
}

/**
 * Read data from table by spec.
 *
 * @param tableSpec a spec for the table
 * @return collection of table rows
 */
inline fun BigQueryReader.table(tableSpec: String): PCollection<TableRow> {
    val reader = getConfigurator().invoke(BigQueryIO.readTableRows().from(tableSpec))
    return pipeline.apply(reader.hashWithName("BigQuery().table($tableSpec)"), reader)
}

/**
 * Read data from table by reference.
 *
 * @param tableRef a reference to the table
 * @return collection of table rows
 */
inline fun BigQueryReader.table(tableRef: TableReference): PCollection<TableRow> {
    val reader = getConfigurator().invoke(BigQueryIO.readTableRows().from(tableRef))
    return pipeline.apply(reader.hashWithName("BigQuery().table($tableRef)"), reader)
}
