import kafka.serializer.{Decoder, Encoder}
import kafka.utils.VerifiableProperties
import org.jnetpcap.packet.PcapPacket


class PcapEncoder(props: VerifiableProperties) extends Encoder[PcapPacket] {
   def toBytes(packet:PcapPacket):Array[Byte] = {
     val tobyte = new Array[Byte](packet.getTotalSize())
     packet.transferStateAndDataTo(tobyte)
     tobyte
   }
}
class PcapDecoder(props: VerifiableProperties) extends Decoder[PcapPacket] {
    def fromBytes(bytes:Array[Byte]) : PcapPacket = {
      val packet = new PcapPacket(bytes)
      packet
     }
}


