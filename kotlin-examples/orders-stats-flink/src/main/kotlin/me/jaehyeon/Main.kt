package me.jaehyeon

import mu.KotlinLogging
import kotlin.system.exitProcess

private val logger = KotlinLogging.logger {}

fun main(args: Array<String>) {
    try {
        when (args.getOrNull(0)?.lowercase()) {
            "datastream" -> DataStreamApp.run()
            "table" -> TableApp.run()
            else -> println("Usage: <datastream | table>")
        }
    } catch (e: Exception) {
        logger.error(e) { "Fatal error in ${args.getOrNull(0) ?: "app"}. Shutting down." }
        exitProcess(1)
    }
}
