package org.bgprocess.eventual

import java.net.InetAddress
import java.net.InetSocketAddress
import java.util.logging.LogManager
import java.util.logging.Logger
import java.util.logging.SimpleFormatter
import java.util.logging.StreamHandler

fun main(args: Array<String>) {
    Logger.getGlobal().addHandler(StreamHandler(System.out, SimpleFormatter()))
    val server = Server(InetSocketAddress(InetAddress.getLocalHost(), 4455), MemoryStorage())

    server.start()
}