package me.jaehyeon

fun main(args: Array<String>) {
    when (args.getOrNull(0)?.lowercase()) {
        "producer" -> ProducerApp.run()
        "consumer" -> ConsumerApp.run()
        else -> println("Usage: <producer|consumer>")
    }
}
