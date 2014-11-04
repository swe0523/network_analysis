/*
/**
 * Created by root on 26/10/14.
 */
import org.jnetpcap.PcapHeader
import org.jnetpcap.nio.JMemory.Type
import org.jnetpcap.packet.JPacket


abstract class KafkaDecoder(buffer: Array[Byte]) extends JPacket(Type.POINTER) {

  private val header = new PcapHeader(Type.POINTER)

  transferStateAndDataTo(buffer)

  def transferStateAndDataTo(buffer: Array[Byte]): Int = {
    var o = header.transferTo(buffer, 0)
    o += state.transferTo(buffer, o)
    o += super.transferTo(buffer, 0, size, o)
    o
  }
}*/
