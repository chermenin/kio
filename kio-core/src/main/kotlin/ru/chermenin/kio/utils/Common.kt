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

package ru.chermenin.kio.utils

import java.util.*
import ru.chermenin.kio.Kio

/**
 * Default toString() implementation with a name as a prefix.
 * Example: "world".hashWithName("hello") => hello@6c11b92
 */
fun Any.hashWithName(name: String): String {
    return name + '@' + Integer.toHexString(hashCode())
}

/**
 * Get version of Kio components by name.
 *
 * @param componentName the name of the Kio component
 * @return version as a string value
 */
internal fun getVersion(componentName: String): String {
    val properties = Properties()
    properties.load(Kio::class.java.classLoader.getResourceAsStream("version.properties"))
    return properties.getProperty("$componentName.version")
}
