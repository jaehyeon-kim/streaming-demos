package me.jaehyeon

import me.jaehyeon.model.User
import me.jaehyeon.serializer.JsonDeserializer
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.Properties

object KafkaConsumerApp {
    private val logger = KotlinLogging.logger { }

    fun run() {
        val props =
            Properties().apply {
                put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("BOOTSTRAP") ?: "localhost:9092")
                put(ConsumerConfig.GROUP_ID_CONFIG, "users-json-group")
                put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
                put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, User::class.java.name)
                put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            }

        val consumer = KafkaConsumer<String, User>(props, StringDeserializer(), JsonDeserializer(User::class.java))
        consumer.use {
            it.subscribe(listOf(System.getenv("TOPIC") ?: "users-json"))
            while (true) {
                val records = it.poll(Duration.ofMillis(1000))
                for (record in records) {
                    logger.info { "Received: ${record.value()}" }
                }
            }
        }
    }
}
