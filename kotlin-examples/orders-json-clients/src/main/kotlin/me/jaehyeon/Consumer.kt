package me.jaehyeon

import me.jaehyeon.model.Order
import me.jaehyeon.serializer.JsonDeserializer
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.Properties

object Consumer {
    private val bootstrapAddress = System.getenv("BOOTSTRAP_ADDRESS") ?: "localhost:9092"
    private val topicName = System.getenv("TOPIC_NAME") ?: "orders-json"
    private val logger = KotlinLogging.logger { }
    private const val MAX_RETRIES = 3
    private const val ERROR_THRESHOLD = -1

    @Volatile
    private var keepConsuming = true

    fun run() {
        val props =
            Properties().apply {
                put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress)
                put(ConsumerConfig.GROUP_ID_CONFIG, "$topicName-group")
                put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
                put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Order::class.java.name)
                put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
                put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            }

        val consumer =
            KafkaConsumer<String, Order>(
                props,
                StringDeserializer(),
                JsonDeserializer(Order::class.java),
            )

        Runtime.getRuntime().addShutdownHook(
            Thread {
                logger.info("Shutdown detected. Waking up Kafka consumer...")
                keepConsuming = false
                consumer.wakeup()
            },
        )

        try {
            consumer.subscribe(listOf(topicName))
            while (keepConsuming) {
                try {
                    val records = consumer.poll(Duration.ofMillis(1000))
                    for (record in records) {
                        processRecordWithRetry(record)
                    }
                } catch (e: WakeupException) {
                    if (keepConsuming) throw e
                    logger.info("Consumer wakeup for shutdown.")
                } catch (e: Exception) {
                    logger.error(e) { "Unexpected error while polling records" }
                }
            }
        } catch (e: Exception) {
            logger.error(e) { "Fatal error in Kafka consumer, existing..." }
        } finally {
            logger.info { "Closing Kafka consumer..." }
            try {
                consumer.close(Duration.ofSeconds(10))
                logger.info { "Kafka consumer closed successfully." }
            } catch (e: Exception) {
                logger.error(e) { "Error occurred while closing Kafka consumer" }
            }
        }
    }

    private fun processRecordWithRetry(record: ConsumerRecord<String, Order>) {
        var attempt = 0
        while (attempt < MAX_RETRIES) {
            try {
                attempt++
                if ((0..99).random() < ERROR_THRESHOLD) {
                    throw RuntimeException(
                        "Simulated error for ${record.value()} from partition ${record.partition()}, offset ${record.offset()}",
                    )
                } else {
                    logger.info { "Received ${record.value()} from partition ${record.partition()}, offset ${record.offset()}" }
                }
                return
            } catch (e: Exception) {
                logger.warn(e) { "Error processing record (attempt $attempt of $MAX_RETRIES)" }
                if (attempt >= MAX_RETRIES) {
                    logger.error(e) { "Failed to process record after $MAX_RETRIES attempts, skipping..." }
                } else {
                    Thread.sleep(500 * attempt.toLong()) // exponential backoff
                }
            }
        }
    }
}
