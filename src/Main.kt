import utils.Device
import utils.NetworkAddress

fun main(args : Array<String>) {
    val addr = NetworkAddress("10.172.104.35", 24105)
    val device = Device("lrb","dar","temple", "", "", addr, "philips_ethernet")

    println("Connecting to " + device.deviceIdentity.addrString + "...")
    device.run()

    val serializer = MsgpackSerializer()
    val writer = ZeromqWriter(serializer)
    device.addListener(writer)

    while (true) {
        Thread.sleep(600000)
    }
}