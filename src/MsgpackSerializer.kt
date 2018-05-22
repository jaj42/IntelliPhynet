import java.nio.ByteBuffer
import org.msgpack.jackson.dataformat.MessagePackFactory
import com.fasterxml.jackson.databind.ObjectMapper

// LightICE imports
import common.DeviceIdentity
import datatypes.Data
import datatypes.Numeric
import datatypes.SampleArray
import export.serializers.Serializer

class MsgpackSerializer(flat: Boolean) : Serializer(ByteBuffer::class.java, flat) {
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
        val bytevalues = serializeToBytes(deviceIdentity, data)
        val hexstrings = bytevalues.map{bytesToHex(it)}
        return hexstrings
    }

    override fun serializeToBytes(deviceIdentity: DeviceIdentity, data: Data): List<ByteArray> =
            if (this.flat)
                throw(Exception("Flat not implemented for now in MsgpackSerializer"))
            else when (data) {
                is Numeric -> serializeNumerics(data)
                is SampleArray -> serializeWaveform(data)
                else -> emptyList()
            }

    fun serializeNumerics(data: Numeric): List<ByteArray> {
        val metric = if (data.rosettaMetric != "") data.rosettaMetric else data.vendorMetric
        val dataMap: HashMap<String, Float> = HashMap()

        dataMap[metric] = data.value

        val bytes = objectMapper.writeValueAsBytes(dataMap)
        return listOf(bytes)
    }

    fun serializeWaveform(data: SampleArray): List<ByteArray> {
        //val timestampsNano = data.getTimestampsDeviceTime(false)

        val metric = if (data.rosettaMetric != "") data.rosettaMetric else data.vendorMetric
        val dataMap: HashMap<String, FloatArray> = HashMap()

        dataMap[metric] = data.getValues()

        val bytes = objectMapper.writeValueAsBytes(dataMap)
        return listOf(bytes)
    }
}