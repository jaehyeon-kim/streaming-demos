package me.jaehyeon.flink.watermark

import mu.KotlinLogging
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.types.Row

object RowWatermarkStrategy {
    private val logger = KotlinLogging.logger {} // Ensure logger is available

    val strategy: WatermarkStrategy<Row> =
        WatermarkStrategy
            .forBoundedOutOfOrderness<Row>(java.time.Duration.ofSeconds(5)) // Same lateness
            .withTimestampAssigner { row: Row, _ ->
                try {
                    // Get the field by index. Assumes bid_time is at index 1 and is Long.
                    val timestamp = row.getField(1) as? Long
                    if (timestamp != null) {
                        timestamp
                    } else {
                        logger.warn { "Null or invalid timestamp at index 1 in Row: $row. Using current time." }
                        System.currentTimeMillis() // Fallback
                    }
                } catch (e: Exception) {
                    // Catch potential ClassCastException or other issues
                    logger.error(e) { "Error accessing timestamp at index 1 in Row: $row. Using current time." }
                    System.currentTimeMillis() // Fallback
                }
            }.withIdleness(java.time.Duration.ofSeconds(10)) // Same idleness
}
