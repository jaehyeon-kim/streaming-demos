package me.jaehyeon

fun main(args: Array<String>) {
    when (args.getOrNull(0)?.lowercase()) {
        "producer" -> KafkaProducerApp.run()
        "consumer" -> KafkaConsumerApp.run()
        else -> println("Usage: <producer|consumer>")
    }
}
