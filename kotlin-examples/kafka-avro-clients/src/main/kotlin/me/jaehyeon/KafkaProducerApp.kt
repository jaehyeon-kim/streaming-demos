@file:Suppress("ktlint:standard:no-wildcard-imports")

package me.jaehyeon

import me.jaehyeon.avro.User
import mu.KotlinLogging
import net.datafaker.Faker
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.*

object KafkaProducerApp {
    private val logger = KotlinLogging.logger {}
    private val faker = Faker()

    fun run() {
        val props =
            Properties().apply {
                put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("BOOTSTRAP") ?: "localhost:9092")
                put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
                put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer")
                put("schema.registry.url", System.getenv("REGISTRY_URL") ?: "http://localhost:8081")
                put("basic.auth.credentials.source", "USER_INFO")
                put("basic.auth.user.info", "admin:admin")
            }

        KafkaProducer<String, User>(props).use { producer ->
            while (true) {
                val user =
                    User(
                        faker.name().fullName(),
                        faker.internet().emailAddress(),
                        faker.number().numberBetween(20, 60),
                    )
                val record =
                    ProducerRecord(
                        System.getenv("TOPIC") ?: "users-avro",
                        user.email,
                        user,
                    )
                producer.send(record) { metadata, exception ->
                    if (exception != null) {
                        logger.error(exception) { "Error sending record" }
                    } else {
                        logger.info { "Sent to ${metadata.topic()} [${metadata.partition()}] @ ${metadata.offset()}" }
                    }
                }
                Thread.sleep(1000)
            }
        }
    }
}
