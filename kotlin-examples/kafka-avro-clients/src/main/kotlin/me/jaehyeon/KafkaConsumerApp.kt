@file:Suppress("ktlint:standard:no-wildcard-imports")

package me.jaehyeon

import mu.KotlinLogging
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import java.util.*

object KafkaConsumerApp {
    private val bootstrapAddress = System.getenv("BOOTSTRAP") ?: "localhost:9092"
    private val topicName = System.getenv("TOPIC") ?: "users-avro"
    private val registryUrl = System.getenv("REGISTRY_URL") ?: "http://localhost:8081"
    private val logger = KotlinLogging.logger { }

    fun run() {
        val props =
            Properties().apply {
                put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress)
                put(ConsumerConfig.GROUP_ID_CONFIG, "$topicName-group")
                put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
                put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer")
                put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
                put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                put("specific.avro.reader", false)
                put("schema.registry.url", registryUrl)
                put("basic.auth.credentials.source", "USER_INFO")
                put("basic.auth.user.info", "admin:admin")
            }

        KafkaConsumer<String, GenericRecord>(props).use { consumer ->
            consumer.subscribe(listOf(topicName))
            while (true) {
                val records = consumer.poll(Duration.ofMillis(1000))
                for (record in records) {
                    logger.info { "Received ${record.value()} from partition ${record.partition()}, offset ${record.offset()}" }
                }
            }
        }
    }
}
