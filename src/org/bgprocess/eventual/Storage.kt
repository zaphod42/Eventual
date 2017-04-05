package org.bgprocess.eventual

import java.util.*

interface Storage {
    fun stream(streamName: String): Stream

}

interface Stream {
    fun write(id: UUID, data: ByteArray): WriteResult
    fun write(headId: UUID, id: UUID, data: ByteArray): WriteResult
    fun read(startId: UUID, length: Int, function: (Event) -> Unit)
}

data class Event(val id: UUID, val data: ByteArray)

data class WriteResult(val success: Boolean, val message: String) {
    companion object {
        val successful = WriteResult(true, "")
    }
}

