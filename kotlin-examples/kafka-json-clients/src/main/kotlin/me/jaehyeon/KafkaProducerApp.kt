package me.jaehyeon

import me.jaehyeon.model.User
import me.jaehyeon.serializer.JsonSerializer
import mu.KotlinLogging
import net.datafaker.Faker
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.errors.TopicExistsException
import java.util.Properties

object KafkaProducerApp {
    private val bootstrapAddress = System.getenv("BOOTSTRAP") ?: "localhost:9092"
    private val topicName = System.getenv("TOPIC") ?: "users-json"
    private val logger = KotlinLogging.logger { }
    private val faker = Faker()

    fun run() {
        // create a kafka topic is not existing
        createKafkaTopic(3, 3)

        val props =
            Properties().apply {
                put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress)
                put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
                put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer::class.java.name)
            }

        KafkaProducer<String, User>(props).use { producer ->
            while (true) {
                val user =
                    User(
                        name = faker.name().fullName(),
                        email = faker.internet().emailAddress(),
                        age = faker.number().numberBetween(20, 60),
                    )
                val record =
                    ProducerRecord(
                        topicName,
                        user.email,
                        user,
                    )
                producer.send(record) { metadata, exception ->
                    if (exception != null) {
                        logger.error(exception) { "Error sending record" }
                    } else {
                        logger.info { "Sent to ${metadata.topic()} into partition ${metadata.partition()}, offset ${metadata.offset()}" }
                    }
                }
                Thread.sleep(1000)
            }
        }
    }

    private fun createKafkaTopic(
        numPartitions: Int,
        replicationFactor: Short,
    ) {
        val props =
            Properties().apply {
                put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress)
            }
        AdminClient.create(props).use { client ->
            val newTopic = NewTopic(topicName, numPartitions, replicationFactor)
            val result = client.createTopics(listOf(newTopic))
            try {
                result.all().get() // wait to complete
                logger.info { "Topic '$topicName' created successfully!" }
            } catch (e: Exception) {
                if (e.cause is TopicExistsException) {
                    logger.warn { "Topic '$topicName' already exists. Skipping creation..." }
                } else {
                    logger.error { "Fails to create topic '$topicName': ${e.message}" }
                }
            }
        }
    }
}
