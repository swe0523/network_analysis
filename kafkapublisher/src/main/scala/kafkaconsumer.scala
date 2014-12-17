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
    val ip = new Ip4()
    val tcp = new Tcp()
    val udp = new Udp()
    val icmp = new Icmp()
    val arp = new Arp()
    val Array(zkQuorum, group, topics, numThreads) = args
    val kafkaParams = Map[String, String]("zookeeper.connect" -> zkQuorum, "group.id" -> group)
    val sparkConf = new SparkConf().setMaster("local").setAppName("kafkaconsumer")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
   // ssc.checkpoint("checkpoint")
    val outfile = new File("/home/swetha/Desktop/Resultlive11.txt" )
    //if (outfile.exists()) outfile.delete()
    val topicpMap = topics.split(",").map((_,numThreads.toInt)).toMap

    val lines = KafkaUtils.createStream[String, PcapPacket, StringDecoder, PcapDecoder](ssc, kafkaParams, Map(topics -> 1), StorageLevel.MEMORY_ONLY)
    // val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicpMap) //[String,String,StringDecoder,StringDecoder].toString()
    // val x = lines.foreachRDD(rdd => rdd.map(x=>x._2).foreach(x=>{val y = x.getTotalSize ; println("Size"+y)}))
    /*val TotalSize = lines.map(_._2).map{stream => stream.getTotalSize }
    TotalSize.print()
    */

   var protocol : String ="null"
    var sourceip : String = "null"
    var destinationip : String = "null"

    var count = 0
    var count1 = 0
    val header = lines.map(_._2).map {
      packet => if (packet.hasHeader(tcp)) { if (packet.hasHeader(ip)) {
        protocol = "TCP"
        val ReceivedTime = new Date(packet.getCaptureHeader.timestampInMillis());
         sourceip = FormatUtils.ip(ip.source())
         destinationip = FormatUtils.ip(ip.destination())
        if(tcp.source == 23)
          protocol = "TELNET"
        else if(tcp.source == 80)
          protocol == "HTTP"
        else if(tcp.source == 443)
          protocol == "HTTPS"
     println(protocol + "\t" + ReceivedTime + "\t" + sourceip + "\t" + destinationip + "\t" + tcp.source() + "\t" + tcp.destination())
   // Files.append(counts + "\n", outputFile, Charset.defaultCharset())
      Files.append("\n"+protocol+"\t"+packet.getTotalSize+"\t"+ReceivedTime+"\t"+sourceip+"\t"+destinationip+"\t"+tcp.source()+"\t"+tcp.destination(),outfile,Charset.defaultCharset())
      }
    }
       if (packet.hasHeader(udp)) { if (packet.hasHeader(ip)) {
         protocol = "UDP"
         val ReceivedTime = new Date(packet.getCaptureHeader.timestampInMillis());
         sourceip = FormatUtils.ip(ip.source())
         destinationip = FormatUtils.ip(ip.destination())
        if(udp.source == 53)
          protocol = "DNS"
      println(protocol + "\t" + ReceivedTime + "\t" + sourceip + "\t" + destinationip + "\t" + udp.source() + "\t" + udp.destination())
         Files.append("\n"+protocol+"\t"+packet.getTotalSize+"\t"+ReceivedTime+"\t"+sourceip+"\t"+destinationip+"\t"+udp.source()+"\t"+udp.destination(),outfile,Charset.defaultCharset())
     }
    }
      if (packet.hasHeader(icmp)) { if(packet.hasHeader(ip)) {
     protocol = "ICMP"
      val ReceivedTime = new Date(packet.getCaptureHeader.timestampInMillis())
       sourceip = FormatUtils.ip(ip.source())
      destinationip = FormatUtils.ip(ip.destination())
      println(protocol + "\t" + ReceivedTime + "\t" + sourceip + "\t" + destinationip)

    Files.append("\n"+protocol+"\t"+packet.getTotalSize+"\t"+ReceivedTime+"\t"+sourceip+"\t"+destinationip,outfile,Charset.defaultCharset())
      }}
       if (packet.hasHeader(arp)) {
        val protocol = "ARP"
        val ReceivedTime = new Date(packet.getCaptureHeader.timestampInMillis())
        val sourceip = FormatUtils.ip(ip.source())
        val destinationip = FormatUtils.ip(ip.destination())
        println(protocol + "\t" + ReceivedTime + "\t" + sourceip + "\t" + destinationip)
        Files.append("\n"+protocol+"\t"+packet.getTotalSize+"\t"+ReceivedTime+"\t"+sourceip+"\t"+destinationip+"\t"+arp.protocolTypeDescription()+"\t"+arp.hardwareTypeDescription(),outfile,Charset.defaultCharset())
      }



      else {
        lines.print()
      }
        if(protocol == "TCP"){
          if(sourceip == destinationip){
            count += 1
            if(count>20){
              println("LAND ATTACK")
              Files.append("LAND ATTACK--------------------------"+sourceip,outfile,Charset.defaultCharset())
            }
          }  }
        if(protocol == "TCP"){
          if(destinationip == "10.30.59.255" && sourceip == "10.30.56.201"){
            count1 += 1
            if(count1>20){
              print("SMURF ATTACK-------------------------")
            }
          }
        }

    }
   header.print()
   // header.saveAsTextFiles("/home/swetha/Desktop/Result1.txt")
    /* val header = words.map{stream => stream.getCaptureHeader.wirelen()}
    header.print()*/
    ssc.start()
    ssc.awaitTermination()
  }
}
