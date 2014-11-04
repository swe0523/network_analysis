/*
import kafka.serializer.Encoder
import kafka.utils.VerifiableProperties
import org.jnetpcap.packet.PcapPacket

class PcapEncoder(verifiableProperties: VerifiableProperties) extends Encoder[PcapPacket] {

  override def toBytes(customMessage: PcapPacket): Array[Byte] = customMessage.transferStateAndDataFrom(PcapPacket : Array[Byte])
}
*/

/*
trait Encoder[T] {
  def toBytes(t: T): Array[Byte]
}

class DefaultEncoder(props: VerifiableProperties = null) extends Encoder[Array[Byte]] {
  override def toBytes(value: Array[Byte]): Array[Byte] = value
}
class NullEncoder[T](props: VerifiableProperties = null) extends Encoder[T] {
  override def toBytes(value: T): Array[Byte] = null
}

class StringEncoder(props: VerifiableProperties = null) extends Encoder[String] {
  val encoding =
    if(props == null)
      "UTF8"
    else
      props.getString("serializer.encoding", "UTF8")
  override def toBytes(s: String): Array[Byte] =
    if(s == null)
      null
    else
      s.getBytes(encoding)
}
abstract class PcapEncoder(props: VerifiableProperties = null) extends Encoder[Array[Byte]] {

  //private val header = new PcapHeader(Type.POINTER)

  println("Inside Encoder")
  //transferStateAndDataFrom(buffer)

 /* def transferStateAndDataFrom(buffer: Array[Byte]): Int = {
    val b = getMemoryBuffer(buffer)
    peerStateAndData(b, 0)
  }

  private def peerStateAndData(memory: JBuffer, offset: Int): Int = {
    var o = header.peer(memory, offset)
    state.peerTo(memory, offset + o, State.sizeof(0))
    o += state.peerTo(memory, offset + o, State.sizeof(state.getHeaderCount))
    o += super.peer(memory, offset + o, header.caplen())
    o
  }*/
}*/
