package me.jaehyeon

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import me.jaehyeon.avro.SupplierStats
import me.jaehyeon.streams.extractor.BidTimeTimestampExtractor
import me.jaehyeon.streams.processor.LateRecordProcessor
import mu.KotlinLogging
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Branched
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Grouped
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.TimeWindows
import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import java.time.Duration
import java.util.Properties

object StreamsApp {
    private val bootstrapAddress = System.getenv("BOOTSTRAP") ?: "localhost:9092"
    private val inputTopicName = System.getenv("TOPIC") ?: "orders-avro"
    private val registryUrl = System.getenv("REGISTRY_URL") ?: "http://localhost:8081"
    private const val NUM_PARTITIONS = 3
    private const val REPLICATION_FACTOR: Short = 3
    private val logger = KotlinLogging.logger {}

    fun run() {
        // create output topics if not existing
        val outputTopicName = "$inputTopicName-stats"
        val skippedTopicName = "$outputTopicName-skipped"
        listOf(outputTopicName, skippedTopicName).forEach { name ->
            createTopicIfNotExists(name)
        }

        val props =
            Properties().apply {
                put(StreamsConfig.APPLICATION_ID_CONFIG, "$outputTopicName-streams")
                put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress)
                put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String()::class.java.name)
                put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest")
                put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, BidTimeTimestampExtractor::class.java.name)
                put("schema.registry.url", registryUrl)
                put("basic.auth.credentials.source", "USER_INFO")
                put("basic.auth.user.info", "admin:admin")
            }

        val keySerde = Serdes.String()
        val valueSerde =
            GenericAvroSerde().apply {
                configure(
                    mapOf(
                        "schema.registry.url" to registryUrl,
                        "basic.auth.credentials.source" to "USER_INFO",
                        "basic.auth.user.info" to "admin:admin",
                    ),
                    false,
                )
            }
        val supplierStatsSerde =
            SpecificAvroSerde<SupplierStats>().apply {
                configure(
                    mapOf(
                        "schema.registry.url" to registryUrl,
                        "basic.auth.credentials.source" to "USER_INFO",
                        "basic.auth.user.info" to "admin:admin",
                    ),
                    false,
                )
            }

        val builder = StreamsBuilder()
        val source: KStream<String, GenericRecord> = builder.stream(inputTopicName, Consumed.with(keySerde, valueSerde))

        val windowSize = Duration.ofSeconds(5)
        val gradePeriod = Duration.ZERO

        val taggedStream: KStream<String, Pair<GenericRecord, Boolean>> =
            source.process(
                ProcessorSupplier {
                    LateRecordProcessor(windowSize, gradePeriod)
                },
                Named.`as`("process-late-records"),
            )

        val branches: Map<String, KStream<String, Pair<GenericRecord, Boolean>>> =
            taggedStream
                .split(Named.`as`("branch-"))
                .branch({ _, value -> !value.second }, Branched.`as`("valid")) // Not late: value.second is false
                .branch({ _, value -> value.second }, Branched.`as`("late")) // Late: value.second is true
                .noDefaultBranch()

        val validSource: KStream<String, GenericRecord> =
            branches["branch-valid"]!!
                .mapValues { _, pair -> pair.first }

        val lateSource: KStream<String, GenericRecord> =
            branches["branch-late"]!!
                .mapValues { _, pair -> pair.first }

        lateSource
            .peek { key, value ->
                logger.warn { "Routing potentially late record to $skippedTopicName: key=$key, bid_time=${value["bid_time"]}" }
            }.to(skippedTopicName, Produced.with(keySerde, valueSerde))

        val aggregated: KTable<Windowed<String>, SupplierStats> =
            validSource
                .map { _, value ->
                    val supplier = value["supplier"]?.toString() ?: "UNKNOWN"
                    val price = value["price"] as? Double ?: 0.0
                    KeyValue(supplier, price)
                }.groupByKey(Grouped.with(keySerde, Serdes.Double()))
                .windowedBy(TimeWindows.ofSizeAndGrace(windowSize, gradePeriod))
                .aggregate(
                    {
                        SupplierStats
                            .newBuilder()
                            .setWindowStart("")
                            .setWindowEnd("")
                            .setSupplier("")
                            .setTotalPrice(0.0)
                            .setCount(0L)
                            .build()
                    },
                    { key, value, aggregate ->
                        val updated =
                            SupplierStats
                                .newBuilder(aggregate)
                                .setSupplier(key)
                                .setTotalPrice(aggregate.totalPrice + value)
                                .setCount(aggregate.count + 1)
                        updated.build()
                    },
                    Materialized.with(keySerde, supplierStatsSerde),
                )
        aggregated
            .toStream()
            .map { key, value ->
                val windowStart = key.window().startTime().toString()
                val windowEnd = key.window().endTime().toString()
                val updatedValue =
                    SupplierStats
                        .newBuilder(value)
                        .setWindowStart(windowStart)
                        .setWindowEnd(windowEnd)
                        .build()
                KeyValue(key.key(), updatedValue)
            }.peek { _, value ->
                logger.info {
                    "Window Start: ${value.windowStart}, Window End: ${value.windowEnd}, Supplier: ${value.supplier}, Total Price: ${value.totalPrice}, Count: ${value.count}"
                }
            }.to(outputTopicName, Produced.with(keySerde, supplierStatsSerde))

        val streams = KafkaStreams(builder.build(), props)

        try {
            streams.start()
            logger.info { "Kafka Streams started successfully." }

            Runtime.getRuntime().addShutdownHook(
                Thread {
                    logger.info { "Shutting down Kafka Streams..." }
                    streams.close()
                },
            )
        } catch (e: Exception) {
            logger.error(e) { "Error while running Kafka Streams" }
            streams.close(Duration.ofSeconds(5))
            throw e
        }
    }

    private fun createTopicIfNotExists(topicName: String) {
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
}
