import kafka.serializer.Encoder
import kafka.utils.VerifiableProperties
import org.jnetpcap.PcapHeader
import org.jnetpcap.packet.{JPacket, PcapPacket}
import org.jnetpcap.nio.JMemory.Type
/**
 * Created by root on 30/10/14.
 */

trait test extends JPacket {
}


class Codec {
  /*class PcapDecoder(props: VerifiableProperties) extends Decoder[PcapPacket] with JPacket {

 }*/
  class PcapEncoder(props: VerifiableProperties) extends Encoder[PcapPacket] with test{

      def transferStateandDataTo(packet:Array[Byte]) : Int= {
      val header = new PcapHeader(Type.POINTER)
      var o = header.transferTo(packet, 0)
      o += state.transferTo(packet, o)
      o += super.transferTo(packet, 0, size, o)
      o
      }
  }
}
