import java.nio.ByteBuffer
import org.msgpack.jackson.dataformat.MessagePackFactory
import com.fasterxml.jackson.databind.ObjectMapper

// LightICE imports
import common.DeviceIdentity
import datatypes.Data
import datatypes.Numeric
import datatypes.SampleArray
import export.serializers.Serializer

class MsgpackSerializer : Serializer {
    var objectMapper = ObjectMapper(MessagePackFactory())

    constructor(flat: Boolean) : super(ByteBuffer::class.java, flat)

    override fun serializeToString(deviceIdentity: DeviceIdentity, data: Data): List<String> {
        val values = serializeToBytes(deviceIdentity, data)
        return values.map{it.toString()}
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