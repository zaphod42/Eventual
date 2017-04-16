package org.bgprocess.eventual

import java.io.Closeable
import java.nio.ByteBuffer
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.OpenOption
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.time.Instant
import java.util.*
import kotlin.collections.HashMap

class DiskStorage(val dataDir: Path) : Storage {
    val streams = HashMap<String, DiskStream>()

    override fun stream(streamName: String): Stream {
        return streams.getOrPut(streamName, {
            DiskStream(dataDir.resolve(streamName))
        })
    }
}

/**
 * Data file format
 *
 * Version | Event Data Entries: UUID | Write Time (Millis since Epoch) | Data Size | Data      |
 * 1 byte  | 16 bytes                 | 8 bytes                         | 2 bytes   | data size |
 *
 */
class DiskStream(val dataFile: Path) : Stream, Closeable {
    val HEAD_ID = UUID(0, 0)
    val file = FileChannel.open(dataFile, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)
    val index = FileChannel.open(dataFile.parent.resolve(dataFile.fileName.toString() + ".idx"), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)
    var currentHeadId: UUID = HEAD_ID

    override fun read(startId: UUID, length: Int, function: (Event) -> Unit) {
        val buffer = ByteBuffer.allocate(20)
        if(startId != HEAD_ID) {
            for (location in 0..index.size() step 20) {
                index.read(buffer, location)
                // Don't allocate a UUID object for this check. No need for more garbage
                if ((buffer.getLong(0) == startId.mostSignificantBits) and (buffer.getLong(8) == startId.leastSignificantBits)) {
                    playEvents(buffer.getInt(16), function)
                }
            }
        } else {
            playEvents(0, function)
        }
    }

    private fun playEvents(position: Int, function: (Event) -> Unit) {
        var currentPosition = position
        val map = file.map(FileChannel.MapMode.READ_ONLY, position.toLong(), file.size() - position)

        while (currentPosition < file.size()) {
            val size = map.getShort(currentPosition + 24)
            val data = ByteArray(size.toInt())
            map.position(currentPosition + 26)
            map.get(data)
            function(Event(UUID(map.getLong(currentPosition), map.getLong(currentPosition + 8)), data))
            currentPosition += 26 + size
        }
    }

    override fun write(headId: UUID, id: UUID, data: ByteArray): WriteResult {
        if(currentHeadId != headId) {
            return WriteResult(false, "Head not at expected event")
        }

        return write(id, data)
    }

    override fun write(id: UUID, data: ByteArray): WriteResult {
        val position = file.size()
        file.position(position)
        val dataBuffer = ByteBuffer.allocate(16 + 8 + 2 + data.size)
        dataBuffer.putLong(id.mostSignificantBits)
                .putLong(id.leastSignificantBits)
                .putLong(Instant.now().toEpochMilli())
                .putShort(data.size.toShort())
                .put(data)
                .rewind()
        file.write(dataBuffer)
        file.force(false)

        val indexBuffer = ByteBuffer.allocate(16 + 4)
        indexBuffer.putLong(id.mostSignificantBits)
                .putLong(id.leastSignificantBits)
                .putInt(position.toInt())
                .rewind()
        index.write(indexBuffer, index.size())

        currentHeadId = id

        return WriteResult(true, "Wrote ${id}")
    }

    override fun close() {
        file.close()
    }
}

