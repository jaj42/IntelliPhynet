import java.nio.ByteBuffer
import org.msgpack.jackson.dataformat.MessagePackFactory
import com.fasterxml.jackson.databind.ObjectMapper

// LightICE imports
import common.DeviceIdentity
import datatypes.Data
import datatypes.Numeric
import datatypes.SampleArray
import export.serializers.Serializer

class MsgpackSerializer() : Serializer(ByteBuffer::class.java, true) {
    val objectMapper = ObjectMapper(MessagePackFactory())

    private val HEX_CHARS = "0123456789ABCDEF".toCharArray()
    private fun bytesToHex(inbytes: ByteArray): String {
        // From https://gist.github.com/fabiomsr/845664a9c7e92bafb6fb0ca70d4e44fd
        val result = StringBuffer()
        inbytes.forEach {
            val octet = it.toInt()
            val firstIndex = (octet and 0xF0).ushr(4)
            val secondIndex = octet and 0x0F
            result.append(HEX_CHARS[firstIndex])
            result.append(HEX_CHARS[secondIndex])
        }
        return result.toString()
    }

    override fun serializeToString(deviceIdentity: DeviceIdentity, data: Data): List<String> {
        val byteValues = serializeToBytes(deviceIdentity, data)
        return byteValues.map(this::bytesToHex)
    }

    override fun serializeToBytes(deviceIdentity: DeviceIdentity, data: Data): List<ByteArray> =
            when (data) {
                is Numeric -> listOf(serializeNumerics(data))
                is SampleArray -> listOf(serializeWaveform(data))
                else -> emptyList()
            }

    private fun serializeNumerics(data: Numeric): ByteArray {
        val metric = if (data.rosettaMetric != "") data.rosettaMetric else data.vendorMetric
        val devicetime = data.deviceTime.timestampNano()
        val value =  hashMapOf(metric to data.value)
        val tags = emptyMap<Nothing, Nothing>()
        val meta = emptyMap<Nothing, Nothing>()
        val aggregated = hashMapOf("basetime" to devicetime, "data" to value, "tags" to tags, "meta" to meta)

        return objectMapper.writeValueAsBytes(aggregated)
    }

    private fun serializeWaveform(data: SampleArray): ByteArray {
        val metric = if (data.rosettaMetric != "") data.rosettaMetric else data.vendorMetric

        val timestamps = data.getTimestampsDeviceTime(true)
        val values = data.getValues()
        val devicetime = data.deviceTime.timestampNano()
        val values2d =  hashMapOf("time" to timestamps, metric to values)
        val tags = emptyMap<Nothing, Nothing>()
        val meta = hashMapOf("fs" to data.frequency)
        val aggregated = hashMapOf("basetime" to devicetime, "data" to values2d, "tags" to tags, "meta" to meta)

        return objectMapper.writeValueAsBytes(aggregated)
    }
}