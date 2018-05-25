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
                is SampleArray -> serializeWaveform(data)
                else -> emptyList()
            }

    fun serializeNumerics(data: Numeric): ByteArray {
        val metric = if (data.rosettaMetric != "") data.rosettaMetric else data.vendorMetric
        val dataMap: HashMap<String, Float> = HashMap()

        dataMap[metric] = data.value
        return objectMapper.writeValueAsBytes(dataMap)
    }

    fun serializeWaveform(data: SampleArray): List<ByteArray> {
        val metric = if (data.rosettaMetric != "") data.rosettaMetric else data.vendorMetric

        val timestampsNano = data.getTimestampsDeviceTime(true)
        val values = data.getValues()

        // Combine time and data axis
        val zipped = timestampsNano.zip(values.toTypedArray())

        // Convert to HashMap
        val mapList = zipped.map{ (time, value) -> hashMapOf("dt" to time, metric to value) }

        // Serialize to MsgPack
        return mapList.map(objectMapper::writeValueAsBytes)
    }
}