@file:Suppress("ktlint:standard:no-wildcard-imports")

package me.jaehyeon

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import mu.KotlinLogging
import org.apache.avro.Schema
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
    private val schemaRegistryClient: SchemaRegistryClient =
        CachedSchemaRegistryClient(
            System.getenv("REGISTRY_URL") ?: "http://localhost:8081",
            100,
            mapOf(
                "basic.auth.credentials.source" to "USER_INFO",
                "schema.registry.basic.auth.user.info" to "admin:admin",
            ),
        )

    fun run() {
        val props =
            Properties().apply {
                put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress)
                put(ConsumerConfig.GROUP_ID_CONFIG, "$topicName-group")
                put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
                put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer")
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
                    val genericRecord = record.value()
                    val schema = fetchLatestSchema(topic = record.topic())
                    logger.info { "Received: ${record.value()}" }
                }
            }
        }
    }

    private fun fetchLatestSchema(topic: String): Schema {
        val subject = "$topic-value"
        val metadata = schemaRegistryClient.getLatestSchemaMetadata(subject)
        return Schema.Parser().parse(metadata.schema)
    }
}
