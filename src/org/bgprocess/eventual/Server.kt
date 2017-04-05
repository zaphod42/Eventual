package org.bgprocess.eventual

import java.io.InputStream
import java.io.OutputStream
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.nio.charset.Charset
import java.util.concurrent.ForkJoinPool
import java.util.logging.Logger

class Server(val address: InetSocketAddress, val storage: Storage) {
    val workerPool = ForkJoinPool(Runtime.getRuntime().availableProcessors(),
            ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true)
    fun start() {
        val socket = ServerSocket()
        socket.bind(address)

        while(true) {
            val client = socket.accept()
            workerPool.submit(RequestHandler(client, storage))
        }
    }
}

class RequestHandler(val client: Socket, val storage: Storage) : Runnable {
    val logger = Logger.getLogger("Eventual Server")

    override fun run() {
        try {
            client.use { c ->
                val input = c.getInputStream()
                val output = c.getOutputStream()

                /**
                 * Request frame format:
                 *
                 * Frame Header
                 * Version | Request Type | Body
                 * 1 byte  | 1 byte       | determined by Request Type
                 *
                 * Request Type == 1 (Write Event, Expected Head)
                 * Stream Name Length | Stream Name Bytes (UTF-8) | Head UUID | Event UUID | Event Size | Event Data
                 * 2 bytes            | up to 65535 bytes         | 16 bytes  | 16 bytes   | 2 bytes    | up to 65535 bytes
                 *
                 * Request Type == 2 (Write Event, No Expected Head)
                 * Stream Name Length | Stream Name Bytes (UTF-8) | Event UUID | Event Size | Event Data
                 * 2 bytes            | up to 65535 bytes         | 16 bytes   | 2 bytes    | up to 65535 bytes
                 *
                 * Request Type == 3 (Read Events)
                 * Stream Name Length | Stream Name Bytes (UTF-8) | Previous UUID | Read Length
                 * 2 bytes            | up to 65535 bytes         | 16 bytes      | 2 bytes
                 *
                 *
                 * Response frame format:
                 *
                 * Frame Header
                 * Version | Response Type | Body
                 * 1 byte  | 1 byte        | determined by Response Type
                 *
                 * Response Type == 1 (Write Succeeded)
                 * no body
                 *
                 * Response Type == 2 (Write Error)
                 * Error UUID | Error Message Length | Error Message (UTF-8)
                 * 16 bytes   | 2 bytes              | up to 65535 bytes
                 *
                 * Response Type == 3 (Read Result)
                 * Section Type | Section Data
                 * 1 byte       | determined by Section Type
                 *
                 * Section Type and Section Data is repeated until an End Section
                 *
                 * Section Type == 1 (Data Section)
                 * Event UUID | Event Size | Event Data
                 * 16 bytes   | 2 bytes    | up to 65535 bytes
                 *
                 * Section Type == 2 (End Section)
                 * no data
                 */
                input.readByte() //version, ignore.
                val type = input.readByte()

                when (type) {
                    1 -> handleWriteToHead(input, output)
                    2 -> handleWrite(input, output)
                    3 -> handleRead(input, output)
                    else -> handleUnknownRequest(output)
                }
            }
        } catch(t: Throwable) {
            logger.severe(t.message)
            throw t
        }
    }

    private fun  handleWriteToHead(input: InputStream, output: OutputStream) {
        val streamNameLength = input.readWord()
        val streamName = input.readText(streamNameLength)
        val headId = input.readUUID()
        val id = input.readUUID()
        val size = input.readWord()
        val data = ByteArray(size)
        input.read(data) //TODO: handle short read

        logger.info("write to head ($streamName, $headId, $id, $size bytes)")

        sendResult(output, storage.stream(streamName).write(headId, id, data))
    }

    private fun  handleWrite(input: InputStream, output: OutputStream) {
        val streamNameLength = input.readWord()
        val streamName = input.readText(streamNameLength)
        val id = input.readUUID()
        val size = input.readWord()
        val data = ByteArray(size)
        input.read(data) //TODO: handle short read

        logger.info("write ($streamName, $id, $size bytes)")

        sendResult(output, storage.stream(streamName).write(id, data))
    }

    private fun sendResult(output: OutputStream, writeResult: WriteResult) {
        if (writeResult.success) {
            logger.info("write successful")
            output.write(1)
            output.write(1)
        } else {
            logger.info("write failed (${writeResult.message})")
            output.write(1)
            output.write(2)
            val errorMessage = writeResult.message.toByteArray(Charset.forName("UTF-8"))
            output.writeWord(errorMessage.size)
            output.write(errorMessage)
        }
    }

    private fun  handleRead(input: InputStream, output: OutputStream) {
        val streamNameLength = input.readWord()
        val streamName = input.readText(streamNameLength)
        val startId = input.readUUID()
        val length = input.readWord()

        logger.info("read ($streamName, $startId, $length events)")

        output.write(1)
        output.write(3)
        storage.stream(streamName).read(startId, length, { sendDataSection(output, it) })
    }

    private fun  sendDataSection(output: OutputStream, event: Event) {
        output.write(1)
        output.writeUUID(event.id)
        output.writeWord(event.data.size)
        output.write(event.data)
    }

    private fun  handleUnknownRequest(output: OutputStream) {
        sendResult(output, WriteResult(false, "Unknown request type"))
    }
}

