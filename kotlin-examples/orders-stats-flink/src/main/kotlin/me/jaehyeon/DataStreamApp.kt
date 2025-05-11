package me.jaehyeon

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import me.jaehyeon.avro.SupplierStats
import mu.KotlinLogging
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.typeinfo.TypeHint
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.flink.connector.kafka.sink.KafkaSink
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.util.OutputTag
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.errors.TopicExistsException
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.Properties

typealias RecordMap = Map<String, Any?>

object DataStreamApp {
    private val toSkipPrint = System.getenv("TO_SKIP_PRINT")?.toBoolean() ?: true
    private val bootstrapAddress = System.getenv("BOOTSTRAP") ?: "localhost:9092"
    private val inputTopicName = System.getenv("TOPIC") ?: "orders-avro"
    private val registryUrl = System.getenv("REGISTRY_URL") ?: "http://localhost:8081"
    private val registryConfig =
        mapOf(
            "basic.auth.credentials.source" to "USER_INFO",
            "basic.auth.user.info" to "admin:admin",
        )
    private const val INPUT_SCHEMA_SUBJECT = "orders-avro-value"
    private const val OUTPUT_SCHEMA_SUBJECT = "orders-avro-stats"
    private const val NUM_PARTITIONS = 3
    private const val REPLICATION_FACTOR: Short = 3
    private val logger = KotlinLogging.logger {}

    // ObjectMapper for converting late data Map to JSON
    private val objectMapper: ObjectMapper by lazy {
        ObjectMapper().registerKotlinModule()
    }

    fun run() {
        // create output topics if not existing
        val outputTopicName = "$inputTopicName-stats"
        val skippedTopicName = "$inputTopicName-skipped"
        listOf(outputTopicName, skippedTopicName).forEach { name ->
            createTopicIfNotExists(
                bootstrapAddress,
                outputTopicName,
                NUM_PARTITIONS,
                REPLICATION_FACTOR,
            )
        }

        val env = StreamExecutionEnvironment.getExecutionEnvironment()
        env.parallelism = 3

        val inputAvroSchema = getLatestSchema(INPUT_SCHEMA_SUBJECT)
        val ordersGenericRecordSource =
            createOrdersSource(
                topic = inputTopicName,
                groupId = "$inputTopicName-flink-datastream",
                bootstrapAddress = bootstrapAddress,
                registryUrl = registryUrl,
                registryConfig = registryConfig,
                schema = inputAvroSchema, // This is the Avro schema for deserialization
            )

        // 1. Stream of GenericRecords from Kafka
        val genericRecordStream: DataStream<GenericRecord> =
            env
                .fromSource(ordersGenericRecordSource, WatermarkStrategy.noWatermarks(), "KafkaGenericRecordSource")

        // 2. Convert GenericRecord to Map<String, Any?> (RecordMap)
        val recordMapStream: DataStream<RecordMap> =
            genericRecordStream
                .map { genericRecord ->
                    val map = mutableMapOf<String, Any?>()
                    genericRecord.schema.fields.forEach { field ->
                        val value = genericRecord.get(field.name())
                        map[field.name()] = if (value is org.apache.avro.util.Utf8) value.toString() else value
                    }
                    map as RecordMap // Cast to type alias
                }.name("GenericRecordToMapConverter")
                .returns(TypeInformation.of(object : TypeHint<RecordMap>() {}))

        // 3. Define OutputTag for late data (now carrying RecordMap)
        val lateMapOutputTag =
            OutputTag(
                "late-order-records",
                TypeInformation.of(object : TypeHint<RecordMap>() {}),
            )

        // 4. Process the RecordMap stream
        val statsStreamOperator: SingleOutputStreamOperator<SupplierStats> =
            recordMapStream
                .assignTimestampsAndWatermarks(SupplierWatermarkStrategy.strategy)
                .keyBy { recordMap -> recordMap["supplier"].toString() }
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
                .allowedLateness(Duration.ofSeconds(5))
                .sideOutputLateData(lateMapOutputTag)
                .aggregate(SupplierStatsAggregator(), SupplierStatsFunction())
        val statsStream: DataStream<SupplierStats> = statsStreamOperator

        // 5. Handle late data as a pair of key and value
        val lateDataMapStream: DataStream<RecordMap> = statsStreamOperator.getSideOutput(lateMapOutputTag)
        val lateKeyPairStream: DataStream<Pair<String?, String>> =
            lateDataMapStream
                .map { recordMap ->
                    val mutableMap = recordMap.toMutableMap()
                    mutableMap["late"] = true
                    val orderId = mutableMap["order_id"] as? String
                    try {
                        val value = objectMapper.writeValueAsString(mutableMap)
                        Pair(orderId, value)
                    } catch (e: Exception) {
                        logger.error(e) { "Error serializing late RecordMap to JSON: $mutableMap" }
                        val errorJson = "{ \"error\": \"json_serialization_failed\", \"data_keys\": \"${
                            mutableMap.keys.joinToString(
                                ",",
                            )}\" }"
                        Pair(orderId, errorJson)
                    }
                }.returns(TypeInformation.of(object : TypeHint<Pair<String?, String>>() {}))

        if (!toSkipPrint) {
            statsStream
                .print()
                .name("SupplierStatsPrint")
            lateKeyPairStream
                .map { it.second }
                .print()
                .name("LateDataPrint")
        }

        val statsSink =
            createStatsSink(
                topic = outputTopicName,
                bootstrapAddress = bootstrapAddress,
                registryUrl = registryUrl,
                registryConfig = registryConfig,
                outputSubject = OUTPUT_SCHEMA_SUBJECT,
            )

        val skippedSink =
            createSkippedSink(
                topic = skippedTopicName,
                bootstrapAddress = bootstrapAddress,
            )

        statsStream.sinkTo(statsSink).name("SupplierStatsSink")
        lateKeyPairStream.sinkTo(skippedSink).name("LateDataSink")
        env.execute("SupplierStats")
    }

    private fun createOrdersSource(
        topic: String,
        groupId: String,
        bootstrapAddress: String,
        registryUrl: String,
        registryConfig: Map<String, String>,
        schema: Schema,
    ): KafkaSource<GenericRecord> =
        KafkaSource
            .builder<GenericRecord>()
            .setBootstrapServers(bootstrapAddress)
            .setTopics(topic)
            .setGroupId(groupId)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(
                ConfluentRegistryAvroDeserializationSchema.forGeneric(
                    schema,
                    registryUrl,
                    registryConfig,
                ),
            ).setProperties(
                Properties().apply {
                    put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "500")
                },
            ).build()

    private fun createStatsSink(
        topic: String,
        bootstrapAddress: String,
        registryUrl: String,
        registryConfig: Map<String, String>,
        outputSubject: String,
    ): KafkaSink<SupplierStats> =
        KafkaSink
            .builder<SupplierStats>()
            .setBootstrapServers(bootstrapAddress)
            .setKafkaProducerConfig(
                Properties().apply {
                    setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
                    setProperty(ProducerConfig.LINGER_MS_CONFIG, "100")
                    setProperty(ProducerConfig.BATCH_SIZE_CONFIG, (64 * 1024).toString())
                    setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")
                },
            ).setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .setRecordSerializer(
                KafkaRecordSerializationSchema
                    .builder<SupplierStats>()
                    .setTopic(topic)
                    .setKeySerializationSchema { value: SupplierStats ->
                        value.supplier.toByteArray(StandardCharsets.UTF_8)
                    }.setValueSerializationSchema(
                        ConfluentRegistryAvroSerializationSchema.forSpecific<SupplierStats>(
                            SupplierStats::class.java,
                            outputSubject,
                            registryUrl,
                            registryConfig,
                        ),
                    ).build(),
            ).build()

    private fun createSkippedSink(
        topic: String,
        bootstrapAddress: String,
    ): KafkaSink<Pair<String?, String>> =
        KafkaSink
            .builder<Pair<String?, String>>()
            .setBootstrapServers(bootstrapAddress)
            .setKafkaProducerConfig(
                Properties().apply {
                    setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
                    setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")
                    setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
                    setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
                },
            ).setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .setRecordSerializer(
                KafkaRecordSerializationSchema
                    .builder<Pair<String?, String>>()
                    .setTopic(topic)
                    .setKeySerializationSchema { pair: Pair<String?, String> ->
                        pair.first?.toByteArray(StandardCharsets.UTF_8)
                    }.setValueSerializationSchema { pair: Pair<String?, String> ->
                        pair.second.toByteArray(StandardCharsets.UTF_8)
                    }.build(),
            ).build()

    private fun getLatestSchema(schemaSubject: String): Schema {
        val schemaRegistryClient =
            CachedSchemaRegistryClient(
                registryUrl,
                100,
                registryConfig,
            )
        logger.info { "Fetching latest schema for subject '$schemaSubject' from $registryUrl" }
        try {
            val latestSchemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(schemaSubject)
            logger.info {
                "Successfully fetched schema ID ${latestSchemaMetadata.id} version ${latestSchemaMetadata.version} for subject '$schemaSubject'"
            }
            return Schema.Parser().parse(latestSchemaMetadata.schema)
        } catch (e: Exception) {
            logger.error(e) { "Failed to retrieve schema for subject '$schemaSubject' from registry $registryUrl" }
            throw RuntimeException("Failed to retrieve schema for subject '$schemaSubject'", e)
        }
    }

    private fun createTopicIfNotExists(
        bootstrapAddress: String,
        topicName: String,
        numPartitions: Int,
        replicationFactor: Short,
    ) {
        val props =
            Properties().apply {
                put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress)
            }
        val logger = KotlinLogging.logger { }

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
                    logger.error(e) { "Fails to create topic '$topicName': ${e.message}" }
                    throw RuntimeException("Failed to create topic '$topicName'", e)
                }
            }
        }
    }
}

object SupplierWatermarkStrategy {
    private val logger = KotlinLogging.logger {}
    val strategy: WatermarkStrategy<RecordMap> =
        WatermarkStrategy
            .forBoundedOutOfOrderness<RecordMap>(Duration.ofSeconds(5)) // Operates on RecordMap
            .withTimestampAssigner { recordMap, _ ->
                val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                try {
                    val bidTimeString = recordMap["bid_time"]?.toString()
                    if (bidTimeString != null) {
                        val ldt = LocalDateTime.parse(bidTimeString, formatter)
                        ldt.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()
                    } else {
                        logger.warn { "Missing 'bid_time' field in RecordMap: $recordMap. Using processing time." }
                        System.currentTimeMillis()
                    }
                } catch (e: Exception) {
                    logger.error(e) { "Error parsing 'bid_time' from RecordMap: $recordMap. Using processing time." }
                    System.currentTimeMillis()
                }
            }.withIdleness(Duration.ofSeconds(10)) // Optional: if partitions can be idle
}

data class SupplierStatsAccumulator(
    var totalPrice: Double = 0.0,
    var count: Long = 0L,
)

class SupplierStatsAggregator : AggregateFunction<RecordMap, SupplierStatsAccumulator, SupplierStatsAccumulator> {
    override fun createAccumulator(): SupplierStatsAccumulator = SupplierStatsAccumulator()

    override fun add(
        value: RecordMap,
        accumulator: SupplierStatsAccumulator,
    ): SupplierStatsAccumulator =
        SupplierStatsAccumulator(
            accumulator.totalPrice + value["price"] as Double,
            accumulator.count + 1,
        )

    override fun getResult(accumulator: SupplierStatsAccumulator): SupplierStatsAccumulator = accumulator

    override fun merge(
        a: SupplierStatsAccumulator,
        b: SupplierStatsAccumulator,
    ): SupplierStatsAccumulator =
        SupplierStatsAccumulator(
            totalPrice = a.totalPrice + b.totalPrice,
            count = a.count + b.count,
        )
}

class SupplierStatsFunction : WindowFunction<SupplierStatsAccumulator, SupplierStats, String, TimeWindow> {
    companion object {
        private val formatter: DateTimeFormatter =
            DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneId.systemDefault())
    }

    override fun apply(
        supplierKey: String,
        window: TimeWindow,
        input: Iterable<SupplierStatsAccumulator>,
        out: Collector<SupplierStats>,
    ) {
        val accumulator = input.firstOrNull() ?: return
        val windowStartStr = formatter.format(Instant.ofEpochMilli(window.start))
        val windowEndStr = formatter.format(Instant.ofEpochMilli(window.end))

        out.collect(
            SupplierStats
                .newBuilder()
                .setWindowStart(windowStartStr)
                .setWindowEnd(windowEndStr)
                .setSupplier(supplierKey)
                .setTotalPrice(String.format("%.2f", accumulator.totalPrice).toDouble())
                .setCount(accumulator.count)
                .build(),
        )
    }
}
