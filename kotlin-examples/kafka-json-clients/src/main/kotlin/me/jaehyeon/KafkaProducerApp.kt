package me.jaehyeon

import me.jaehyeon.model.Person
import me.jaehyeon.serializer.JsonSerializer
import mu.KotlinLogging
import net.datafaker.Faker
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.Properties

object KafkaProducerApp {
    private val logger = KotlinLogging.logger { }
    private val faker = Faker()

    fun run() {
        val props =
            Properties().apply {
                put("bootstrap.servers", System.getenv("BOOTSTRAP") ?: "localhost:9092")
                put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
                put("value.serializer", JsonSerializer::class.java.name)
            }

        KafkaProducer<String, Person>(props).use { producer ->
            while (true) {
                val person =
                    Person(
                        name = faker.name().fullName(),
                        email = faker.internet().emailAddress(),
                        age = faker.number().numberBetween(20, 60),
                    )
                val record =
                    ProducerRecord(
                        System.getenv("TOPIC") ?: "people-topic",
                        person.email,
                        person,
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
