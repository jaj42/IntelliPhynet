import org.zeromq.ZContext
import org.zeromq.ZMQ

// LightICE imports
import common.DeviceIdentity
import datatypes.Data
import datatypes.SampleArray
import export.serializers.Serializer
import export.writers.Writer

class ZeromqWriter(serializer: Serializer, zmqdest: String = "127.0.0.1") : Writer(serializer) {
    val context = ZContext()
    val waveSocket = context.createSocket(ZMQ.PUB)
    val numSocket = context.createSocket(ZMQ.PUB)

    init {
        numSocket.connect("tcp://$zmqdest:4201")
        waveSocket.connect("tcp://$zmqdest:4202")
    }

    private fun prependTopic(bytes: ByteArray) : ByteArray{
        val topic = "PHI ".toByteArray(ZMQ.CHARSET)
        return topic + bytes
    }
    override fun write(deviceIdentity: DeviceIdentity?, data: Data?) {
        if (data == null)
            return
        val destSocket = if (data is SampleArray) waveSocket else numSocket
        val msgList = this.serializer.serializeToBytes(deviceIdentity, data)
        for (msg in msgList)
            destSocket.send(prependTopic(msg))
    }
}