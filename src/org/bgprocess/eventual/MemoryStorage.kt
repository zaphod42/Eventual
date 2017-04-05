package org.bgprocess.eventual

import java.util.*
import kotlin.collections.HashMap

class MemoryStorage : Storage {
    private val streams = HashMap<String, Stream>()

    override fun stream(streamName: String): Stream {
        return streams.getOrPut(streamName, { MemoryStream() })
    }
}



class MemoryStream : Stream {
    private val events = LinkedList<Event>()

    init {
        events.add(Event(UUID(0, 0), ByteArray(0)))
    }

    override fun write(id: UUID, data: ByteArray): WriteResult {
        events.add(Event(id, data))
        return WriteResult.successful
    }

    override fun write(headId: UUID, id: UUID, data: ByteArray): WriteResult {
        val currentHeadId = events.last.id
        if (currentHeadId == headId) {
            return write(id, data)
        } else {
            return WriteResult(false, "Head needed to be ${headId} but was ${currentHeadId}")
        }
    }

    override fun read(startId: UUID, length: Int, function: (Event) -> Unit) {
        events.dropWhile { it.id != startId }.drop(1).take(length).forEach(function)
    }
}

