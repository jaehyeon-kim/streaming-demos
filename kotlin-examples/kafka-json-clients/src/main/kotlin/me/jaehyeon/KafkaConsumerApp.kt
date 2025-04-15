package me.jaehyeon

import me.jaehyeon.model.Person
import me.jaehyeon.serializer.JsonDeserializer
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.Properties

object KafkaConsumerApp {
    private val logger = KotlinLogging.logger { }

    fun run() {
        val props =
            Properties().apply {
                put("bootstrap.servers", System.getenv("BOOTSTRAP") ?: "localhost:9092")
                put("group.id", "people-consumer")
                put("key.deserializer", StringDeserializer::class.java.name)
                put("value.deserializer", Person::class.java.name)
                put("auto.offset.reset", "earliest")
            }

        val consumer = KafkaConsumer<String, Person>(props, StringDeserializer(), JsonDeserializer(Person::class.java))

        consumer.use {
            it.subscribe(listOf(System.getenv("TOPIC") ?: "people-topic"))
            while (true) {
                val records = it.poll(Duration.ofMillis(1000))
                for (record in records) {
                    logger.info { "Received: ${record.value()}" }
                }
            }
        }
    }
}
