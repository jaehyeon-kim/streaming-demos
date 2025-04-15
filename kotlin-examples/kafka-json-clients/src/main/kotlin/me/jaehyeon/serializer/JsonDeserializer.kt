package me.jaehyeon.serializer

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.common.serialization.Deserializer

class JsonDeserializer<T>(
    private val clazz: Class<T>,
) : Deserializer<T> {
    private val objectMapper = jacksonObjectMapper()

    override fun deserialize(
        topic: String?,
        data: ByteArray?,
    ): T? = data?.let { objectMapper.readValue(it, clazz) }
}
