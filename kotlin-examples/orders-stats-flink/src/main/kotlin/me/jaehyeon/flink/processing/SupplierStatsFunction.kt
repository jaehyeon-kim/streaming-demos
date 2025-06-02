package me.jaehyeon.flink.processing

import me.jaehyeon.avro.SupplierStats
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter

class SupplierStatsFunction : WindowFunction<SupplierStatsAccumulator, SupplierStats, String, TimeWindow> {
    companion object {
        private val formatter: DateTimeFormatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault())
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
