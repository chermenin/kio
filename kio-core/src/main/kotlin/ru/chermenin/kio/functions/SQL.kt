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

package ru.chermenin.kio.functions

import java.lang.IllegalArgumentException
import kotlin.reflect.KProperty
import kotlin.reflect.KType
import kotlin.reflect.full.createType
import kotlin.reflect.full.memberProperties
import org.apache.beam.sdk.coders.RowCoder
import org.apache.beam.sdk.extensions.sql.SqlTransform
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionTuple
import org.apache.beam.sdk.values.Row
import ru.chermenin.kio.utils.hashWithName

private val ByteType: KType = Byte::class.createType()
private val ShortType: KType = Short::class.createType()
private val IntType: KType = Int::class.createType()
private val LongType: KType = Long::class.createType()
private val FloatType: KType = Float::class.createType()
private val DoubleType: KType = Double::class.createType()
private val BooleanType: KType = Boolean::class.createType()
private val StringType: KType = String::class.createType()

fun KProperty<*>.getFieldType(): Schema.FieldType {
    return when (this.returnType) {
        ByteType -> Schema.FieldType.BYTE
        ShortType -> Schema.FieldType.INT16
        IntType -> Schema.FieldType.INT32
        LongType -> Schema.FieldType.INT64
        FloatType -> Schema.FieldType.FLOAT
        DoubleType -> Schema.FieldType.DOUBLE
        BooleanType -> Schema.FieldType.BOOLEAN
        StringType -> Schema.FieldType.STRING
        else -> throw IllegalArgumentException("Unknown type: ${this.returnType}")
    }
}

inline fun <reified T : Any> PCollection<T>.toRows(): PCollection<Row> {
    if (T::class.isData) {

        // prepare schema
        val schema = Schema.builder().addFields(
            T::class.memberProperties.map {
                Schema.Field.of(
                    it.name,
                    it.getFieldType()
                )
            }
        ).build()

        // map elements to rows and apply SQL query
        return this
            .apply(
                schema.hashWithName("rowsPreparation"),
                ParDo.of(
                    object : DoFn<T, Row>() {
                        @ProcessElement
                        fun processElement(context: ProcessContext) {
                            val element = context.element()
                            val values = T::class.memberProperties.map { it.get(element) }
                            val row = Row.withSchema(schema).addValues(values).build()
                            context.output(row)
                        }
                    }
                )
            )
            .setCoder(RowCoder.of(schema))
    } else {
        throw IllegalStateException("SQL query can be applied to the PCollection of data class only.")
    }
}

inline fun PCollection<Row>.sql(query: String): PCollection<Row> {
    val sqlTransform = SqlTransform.query(query)
    return this.apply(sqlTransform.hashWithName("sqlQuery"), sqlTransform)
}

fun with(
    collection: Pair<PCollection<Row>, String>,
    vararg collections: Pair<PCollection<Row>, String>
): PCollectionTuple {
    return collections.fold(
        PCollectionTuple.of(collection.second, collection.first),
        { tuple, pair -> tuple.and(pair.second, pair.first) }
    )
}

inline fun PCollectionTuple.sql(query: String): PCollection<Row> {
    val sqlTransform = SqlTransform.query(query)
    return this.apply(sqlTransform.hashWithName("sqlQuery"), sqlTransform)
}
