// LightICE imports
import export.writers.StdoutWriter
import utils.Conf.parseConfig

fun main(args : Array<String>) {
    val conf = parseConfig("conf.json")
    val device = conf.devices.first() ?: throw IllegalArgumentException("No device defined")

    println("Connecting to " + device.deviceIdentity.alias + "...")
    device.run()

    val serializer = MsgpackSerializer(false)
    val writer = StdoutWriter(serializer)
    device.addListener(writer)

    while (true) {
        Thread.sleep(600000)
    }
}