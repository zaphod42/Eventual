package org.bgprocess.eventual

import java.net.InetAddress
import java.net.InetSocketAddress
import java.nio.file.FileSystem
import java.nio.file.FileSystems
import java.nio.file.Path
import java.util.logging.LogManager
import java.util.logging.Logger
import java.util.logging.SimpleFormatter
import java.util.logging.StreamHandler

fun main(args: Array<String>) {
    Logger.getGlobal().addHandler(StreamHandler(System.out, SimpleFormatter()))
    //val storage = MemoryStorage()
    val storage = DiskStorage(FileSystems.getDefault().getPath("."))
    val server = Server(InetSocketAddress(InetAddress.getLocalHost(), 4455), storage)

    server.start()
}