package me.jaehyeon

fun main(args: Array<String>) {
    when (args.getOrNull(0)?.lowercase()) {
        "producer" -> Producer.run()
        "consumer" -> Consumer.run()
        else -> println("Usage: <producer|consumer>")
    }
}
