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

package ru.chermenin.kio.sql

import org.apache.beam.sdk.schemas.transforms.*
import org.apache.beam.sdk.values.*
import ru.chermenin.kio.functions.*
import ru.chermenin.kio.utils.hashWithName

typealias Dataset<T> = PCollection<T>
typealias DataFrame = Dataset<Row>

private fun Row.toString(truncate: Boolean): String {

    // @todo: change to truncated row strings
    return this.toString()
}

/**
 * Returns all column names and their data types as a map.
 */
fun DataFrame.dtypes(): Map<String, String> {
    return schema.fields.map { field ->
        field.name to field.type.typeName.name
    }.toMap()
}

/**
 * Returns all column names as an array.
 */
fun DataFrame.columns(): Array<String> {
    return schema.fields.map { it.name }.toTypedArray()
}

fun DataFrame.show(numRows: Int, truncate: Boolean) {
    this.limit(numRows).forEach { println(it.toString(truncate)) }
}

fun DataFrame.show(numRows: Int) {
    this.show(numRows, truncate = true)
}

fun DataFrame.show(): Unit {
    this.show(20)
}

fun DataFrame.show(truncate: Boolean): Unit {
    this.show(20, truncate)
}

fun DataFrame.select(vararg cols: String): DataFrame {
    return this.apply(cols.hashWithName("select(${cols.joinToString()})"), Select.fieldNames(*cols))
}

fun DataFrame.filter(conditionExpr: String): DataFrame {

    // @todo: convert from condition
    return this.apply(conditionExpr.hashWithName("filter($conditionExpr)"), Filter.create<Row>())
}

fun DataFrame.where(conditionExpr: String): DataFrame {
    return this.filter(conditionExpr)
}

fun DataFrame.limit(n: Int): DataFrame {
    return this.take(n.toLong())
}

fun DataFrame.withColumnRenamed(existingName: String, newName: String): DataFrame {
    return this.apply(
        existingName.hashWithName("withColumnRenamed($existingName, $newName)"),
        RenameFields.create<Row>().rename(existingName, newName)
    )
}

fun DataFrame.drop(vararg cols: String): DataFrame {
    return this.apply(cols.hashWithName("drop(${cols.joinToString()})"), DropFields.fields(*cols))
}
