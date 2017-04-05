package org.bgprocess.eventual

import java.io.InputStream
import java.io.OutputStream
import java.nio.charset.Charset
import java.util.*

fun InputStream.readByte(): Int {
    return read()
}

fun OutputStream.writeWord(value: Int) {
    write(value shr 8)
    write(value and 0x000000FF)
}

fun InputStream.readWord(): Int {
    val high = readByte()
    val low = readByte()

    return (high shl 8) or low
}

fun InputStream.readDWord(): Int {
    val high = readWord()
    val low = readWord()

    return (high shl 16) or low
}

fun OutputStream.writeQWord(value: Long) {
    var mask = 0xFFL shl (64 - 8)

    for (i in 1..8) {
        write(((value and mask) shr (64 - (8 * i))).toInt())
        mask = mask shr 8
    }
}

fun InputStream.readQWord(): Long {
    val high = readDWord().toLong()
    val low = readDWord().toLong()

    return (high shl 32) or low
}

fun InputStream.readText(length: Int): String {
    val bytes = ByteArray(length)
    read(bytes) //TODO: handle error condition of a short read

    return String(bytes, Charset.forName("UTF-8"))
}

fun OutputStream.writeUUID(value: UUID) {
    writeQWord(value.mostSignificantBits)
    writeQWord(value.leastSignificantBits)
}

fun InputStream.readUUID(): UUID {
    val high = readQWord()
    val low = readQWord()

    return UUID(high, low)
}