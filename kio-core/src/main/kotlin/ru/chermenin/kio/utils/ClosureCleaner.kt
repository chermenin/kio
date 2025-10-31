package ru.chermenin.kio.utils

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream

@Suppress("UNCHECKED_CAST")
object ClosureCleaner {

    fun <T> clean(function: T): T = deserialize(
        serialize(function)
    )

    private fun <T> deserialize(bytes: ByteArray): T {
        ByteArrayInputStream(bytes).use { bis ->
            ObjectInputStream(bis).use {
                return it.readObject() as T
            }
        }
    }

    private fun <T> serialize(obj: T): ByteArray {
        ByteArrayOutputStream().use { bos ->
            ObjectOutputStream(bos).use {
                it.writeObject(obj)
            }
            return bos.toByteArray()
        }
    }
}
