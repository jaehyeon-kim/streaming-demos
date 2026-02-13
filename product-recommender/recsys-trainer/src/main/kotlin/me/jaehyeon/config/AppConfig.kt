package me.jaehyeon.config

import java.io.Serializable

/**
 * centralized Configuration Management.
 * Loads settings from Environment Variables or defaults to local dev settings.
 */
data class AppConfig(
    // Kafka Configuration
    val bootstrapAddress: String = System.getenv("BOOTSTRAP") ?: "kafka-1:19092",
    val registryUrl: String = System.getenv("REGISTRY_URL") ?: "http://schema:8081",
    val registryCredentials: Map<String, String> =
        mapOf(
            "basic.auth.credentials.source" to "USER_INFO",
            "basic.auth.user.info" to "admin:admin",
        ),
    val feedbackTopic: String = "feedback-events",
    // Flink Configuration
    val jobName: String = "RecommenderParameterUpdate",
    val parallelism: Int = 6,
    val checkpointInterval: Long = 10_000,
    val checkPointTimeout: Long = 60_000,
    val minPauseBetweenCheckpoints: Long = 500,
    val maxConcurrentCheckpoints: Int = 1,
    val tolerableCheckpointFailureNumber: Int = 3,
    // Redis Configuration
    val redisHost: String = System.getenv("REDIS_HOST") ?: "redis",
    val redisPort: Int = 6379,
    val redisPass: String = "redis-pass",
    // File Source (Bootstrap)
    val eventLog: String = System.getenv("EVENT_LOG") ?: "file:///tmp/training_log.csv",
) : Serializable
