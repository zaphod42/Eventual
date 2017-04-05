package org.bgprocess.eventual

import java.util.*

class DiskStorage : Storage {
    override fun stream(streamName: String): Stream {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

class DiskStream : Stream {
    override fun read(startId: UUID, length: Int, function: (Event) -> Unit) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun write(headId: UUID, id: UUID, data: ByteArray): WriteResult {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun write(id: UUID, data: ByteArray): WriteResult {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

