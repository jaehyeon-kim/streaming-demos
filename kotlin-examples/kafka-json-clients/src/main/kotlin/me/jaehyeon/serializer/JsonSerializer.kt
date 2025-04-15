package me.jaehyeon.serializer

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.common.serialization.Serializer

class JsonSerializer<T> : Serializer<T> {
    private val objectMapper = jacksonObjectMapper()

    override fun serialize(
        topic: String?,
        data: T?,
    ): ByteArray? = data?.let { objectMapper.writeValueAsBytes(it) }
}
