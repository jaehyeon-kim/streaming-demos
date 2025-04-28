package me.jaehyeon

import me.jaehyeon.model.Order
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
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.Properties
import java.util.UUID
import java.util.concurrent.TimeUnit

object Producer {
    private val bootstrapAddress = System.getenv("BOOTSTRAP_ADDRESS") ?: "localhost:9092"
    private val topicName = System.getenv("TOPIC_NAME") ?: "orders-json"
    private const val NUM_PARTITIONS = 3
    private const val REPLICATION_FACTOR: Short = 3
    private val logger = KotlinLogging.logger { }
    private val faker = Faker()

    fun run() {
        // create a kafka topic if not existing
        createTopic()

        val props =
            Properties().apply {
                put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress)
                put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
                put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer::class.java.name)
            }

        KafkaProducer<String, Order>(props).use { producer ->
            while (true) {
                val order =
                    Order(
                        UUID.randomUUID().toString(),
                        generateBidTime(),
                        faker.number().randomDouble(2, 1, 150),
                        faker.commerce().productName(),
                        faker.regexify("(Alice|Bob|Carol|Alex|Joe|James|Jane|Jack)"),
                    )
                val record = ProducerRecord(topicName, order.orderId, order)
                producer.send(record) { metadata, exception ->
                    if (exception != null) {
                        logger.error(exception) { "Error sending record" }
                    } else {
                        logger.info {
                            "Sent to ${metadata.topic()} into partition ${metadata.partition()}, offset ${metadata.offset()}"
                        }
                    }
                }
                Thread.sleep(1000L)
            }
        }
    }

    private fun createTopic() {
        val props =
            Properties().apply {
                put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress)
            }

        AdminClient.create(props).use { client ->
            val newTopic = NewTopic(topicName, NUM_PARTITIONS, REPLICATION_FACTOR)
            val result = client.createTopics(listOf(newTopic))
            try {
                result.all().get() // wait to complete
                logger.info { "Topic '$topicName' created successfully!" }
            } catch (e: Exception) {
                if (e.cause is TopicExistsException) {
                    logger.warn { "Topic '$topicName' already exists. Skipping creation..." }
                } else {
                    logger.error(e) { "Fails to create topic '$topicName': ${e.message}" }
                    throw RuntimeException("Failed to create topic '$topicName'", e)
                }
            }
        }
    }

    private fun generateBidTime(): String {
        val randomDate = faker.date().past(30, TimeUnit.SECONDS)
        val formatter =
            DateTimeFormatter
                .ofPattern("yyyy-MM-dd HH:mm:ss")
                .withZone(ZoneId.systemDefault())
        return formatter.format(randomDate.toInstant())
    }
}
