package org.bgprocess.eventual

import java.io.*
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.Socket
import java.nio.charset.Charset
import java.util.*

fun main(args: Array<String>) {
   val repl = ClientRepl(Client(InetSocketAddress(InetAddress.getLocalHost(), 4455)), InputStreamReader(System.`in`), OutputStreamWriter(System.out))
    repl.run()
}

class ClientRepl(val client: Client, rawInput: Reader, val output: Writer) {
   val input = BufferedReader(rawInput)
   fun run() {
       while(true) {
           output.write("> ")
           output.flush()
           val expr = Scanner(input.readLine().trim())

           try {
               val command = expr.next()
               if (command == "list") {
                   client.read(UUID(0, 0), expr.next(), 20, {
                       output.append(it.id.toString() + " -> " + String(it.data))
                       output.appendln()
                   })
               } else if (command == "exit") {
                   return
               } else if (command == "write") {
                   val streamName = expr.next()
                   val id = expr.next()
                   val data = expr.nextLine()

                   client.write(streamName, UUID.fromString(id), data.toByteArray(Charset.forName("UTF-8")))
               }
           } catch (e: RuntimeException) {
               output.append("Error: " + e.message)
               output.appendln()
           }
       }
   }
}

class Client(val serverLocation: InetSocketAddress) {
    fun  read(previousId: UUID, streamName: String, size: Int, resultHandler: (Event) -> Unit) {
        Socket(serverLocation.address, serverLocation.port).use { socket ->
            val input = socket.getInputStream()
            val output = socket.getOutputStream()

            output.write(1)
            output.write(3)

            val nameInBytes = streamName.toByteArray(Charset.forName("UTF-8"))
            output.writeWord(nameInBytes.size)
            output.write(nameInBytes)

            output.writeUUID(previousId)
            output.writeWord(size)

            input.read() //Version...don't care
            val response = input.read()

            when (response) {
                3 -> readResults(input, resultHandler)
                else -> throw RuntimeException("Unknown response type $response")
            }
        }
    }

    fun  write(streamName: String, id: UUID, data: ByteArray) {
        Socket(serverLocation.address, serverLocation.port).use { socket ->
            val input = socket.getInputStream()
            val output = socket.getOutputStream()

            output.write(1)
            output.write(2)

            val nameInBytes = streamName.toByteArray(Charset.forName("UTF-8"))
            output.writeWord(nameInBytes.size)
            output.write(nameInBytes)

            output.writeUUID(id)
            output.writeWord(data.size)
            output.write(data)

            input.read() //Version...don't care
            val response = input.read()

            when (response) {
                1 -> return
                2 -> {
                    val errorId = input.readUUID()
                    val errorLength = input.readWord()
                    val errorData = ByteArray(errorLength)
                    input.read(errorData)
                    throw RuntimeException("Error writing event ($errorId): ${String(errorData)}")
                }
                else -> throw RuntimeException("Unknown response type $response")
            }
        }
    }

    private fun  readResults(input: InputStream, resultHandler: (Event) -> Unit) {
        var sectionType = input.read()
        while(sectionType == 1) {
            val uuid = input.readUUID()
            val size = input.readWord()
            val data = ByteArray(size)
            input.read(data)
            resultHandler(Event(uuid, data))

            sectionType = input.read()
        }
    }
}
