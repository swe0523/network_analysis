import java.io.File
import java.nio.charset.Charset
import java.util.Date

import com.google.common.io.Files
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.jnetpcap.packet.PcapPacket
import org.jnetpcap.packet.format.FormatUtils
import org.jnetpcap.protocol.network.{Arp, Icmp, Ip4}
import org.jnetpcap.protocol.tcpip.{Tcp, Udp}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext.rddToPairRDDFunctions
/**
 * Created by root on 11/9/14.
 */
object kafkaconsumer extends Serializable{
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: Kafka <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }
    //zookeeper.connect=127.0.0.1:2181
    //test-consumer-group

    val Array(zkQuorum, group, topics, numThreads) = args
    val kafkaParams = Map[String, String]("zookeeper.connect" -> zkQuorum, "group.id" -> group)
    val sparkConf = new SparkConf().setMaster("local").setAppName("kafkaconsumer")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")
    val outfile = new File("/home/swetha/Desktop/Resultlive11.txt" )
    //if (outfile.exists()) outfile.delete()
    val topicpMap = topics.split(",").map((_,numThreads.toInt)).toMap

    val lines = KafkaUtils.createStream[String, PcapPacket, StringDecoder, PcapDecoder](ssc, kafkaParams, Map(topics -> 1), StorageLevel.MEMORY_ONLY)
   val m= lines.window(Seconds(4), Seconds(4)).mapPartitions(x=>x.map{y=>
   val packet=y._2
      val s=analysis(packet)
      s
   }).filter(x=>x!=())

     .reduceByKeyAndWindow((a, b) => {
      (a :: b)
    }, (a, b) => {
     (a::b)
    }, Seconds(4), Seconds(4))
  .print()

    ssc.start()
    ssc.awaitTermination()
  }
  def analysis(packet:PcapPacket) = {
    val ip = new Ip4()
    val tcp = new Tcp()
    val udp = new Udp()
    val icmp = new Icmp()
    val arp = new Arp()
    var protocol: String = "null"
    var sourceip: String = "null"
    var destinationip: String = "null"
    var sport: Int = 0
    var dport: Int = 0
try {
  if (packet.hasHeader(tcp)) {
    if (packet.hasHeader(ip)) {
      protocol = "TCP"
      val ReceivedTime = new Date(packet.getCaptureHeader.timestampInMillis())
      sourceip = FormatUtils.ip(ip.source())
      destinationip = FormatUtils.ip(ip.destination())
      sport = tcp.source()
      dport = tcp.destination()
      if (sport == 23)
        protocol = "TELNET"
      else if (sport == 80)
        protocol = "HTTP"
      else if (sport == 443)
        protocol = "HTTPS"
    } else ""
  } else
  if (packet.hasHeader(udp)) {
    if (packet.hasHeader(ip)) {
      protocol = "UDP"
      val ReceivedTime = new Date(packet.getCaptureHeader.timestampInMillis());
      sourceip = FormatUtils.ip(ip.source())
      destinationip = FormatUtils.ip(ip.destination())
      sport = udp.source()
      dport = udp.destination()
      if (sport == 53)
        protocol = "DNS"
    } else ""
  } else
  if (packet.hasHeader(icmp)) {
    if (packet.hasHeader(ip)) {
      protocol = "ICMP"
      val ReceivedTime = new Date(packet.getCaptureHeader.timestampInMillis())
      sourceip = FormatUtils.ip(ip.source())
      destinationip = FormatUtils.ip(ip.destination())
      sport = 0
      dport = 0

    } else ""
  } else
  if (packet.hasHeader(arp)) {
    protocol = "ARP"
    val ReceivedTime = new Date(packet.getCaptureHeader.timestampInMillis())
    if (packet.hasHeader(ip)) {
      sourceip = FormatUtils.ip(ip.source())
      destinationip = FormatUtils.ip(ip.destination())
      sport = 0
      dport = 0
    }
  }
  else "no"
}catch{
  case e:Exception=>println(e.getMessage)
}
    (protocol,List(sourceip,destinationip,sport,dport))
  }
}
