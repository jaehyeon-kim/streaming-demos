package me.jaehyeon

import me.jaehyeon.avro.Order
import me.jaehyeon.kafka.createTopicIfNotExists
import mu.KotlinLogging
import net.datafaker.Faker
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.KafkaException
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.Properties
import java.util.UUID
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit

object ProducerApp {
    private val bootstrapAddress = System.getenv("BOOTSTRAP") ?: "localhost:9092"
    private val inputTopicName = System.getenv("TOPIC_NAME") ?: "orders-avro"
    private val registryUrl = System.getenv("REGISTRY_URL") ?: "http://localhost:8081"
    private const val NUM_PARTITIONS = 3
    private const val REPLICATION_FACTOR: Short = 3
    private val logger = KotlinLogging.logger {}
    private val faker = Faker()

    fun run() {
        // Create the input topic if not existing
        createTopicIfNotExists(inputTopicName, bootstrapAddress, NUM_PARTITIONS, REPLICATION_FACTOR)

        val props =
            Properties().apply {
                put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress)
                put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
                put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer")
                put("schema.registry.url", registryUrl)
                put("basic.auth.credentials.source", "USER_INFO")
                put("basic.auth.user.info", "admin:admin")
                put(ProducerConfig.RETRIES_CONFIG, "3")
                put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "3000")
                put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "6000")
                put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "3000")
            }

        KafkaProducer<String, Order>(props).use { producer ->
            while (true) {
                val order =
                    Order().apply {
                        orderId = UUID.randomUUID().toString()
                        bidTime = generateBidTime()
                        price = faker.number().randomDouble(2, 1, 150)
                        item = faker.commerce().productName()
                        supplier = faker.regexify("(Alice|Bob|Carol|Alex|Joe|James|Jane|Jack)")
                    }
                val record = ProducerRecord(inputTopicName, order.orderId, order)
                try {
                    producer
                        .send(record) { metadata, exception ->
                            if (exception != null) {
                                logger.error(exception) { "Error sending record" }
                            } else {
                                logger.info {
                                    "Sent to ${metadata.topic()} into partition ${metadata.partition()}, offset ${metadata.offset()}"
                                }
                            }
                        }.get()
                } catch (e: ExecutionException) {
                    throw RuntimeException("Unrecoverable error while sending record.", e)
                } catch (e: KafkaException) {
                    throw RuntimeException("Kafka error while sending record.", e)
                }

                Thread.sleep(1000L)
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
