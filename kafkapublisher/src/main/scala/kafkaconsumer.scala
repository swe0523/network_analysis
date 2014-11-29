import java.io.PrintWriter
import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.jnetpcap.packet.PcapPacket
import org.jnetpcap.packet.format.FormatUtils
import org.jnetpcap.protocol.network.Ip4
import org.jnetpcap.protocol.tcpip.{Tcp, Udp}


/**
 * Created by root on 11/9/14.
 */

object kafkaconsumer{
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

    val Array(zkQuorum, group, topics, numThreads) = args
    val kafkaParams = Map[String, String]("zookeeper.connect" -> zkQuorum, "group.id" -> group)
    val sparkConf = new SparkConf().setMaster("local").setAppName("kafkaconsumer")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")
    val topicpMap = topics.split(",").map((_,numThreads.toInt)).toMap
    val outfile = new PrintWriter("/home/swetha/Desktop/Result1.txt")
    val lines = KafkaUtils.createStream[String, PcapPacket, StringDecoder, PcapDecoder](ssc, kafkaParams, Map(topics -> 1), StorageLevel.MEMORY_ONLY)
   // val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicpMap) //[String,String,StringDecoder,StringDecoder].toString()

  //  val x = lines.foreachRDD(rdd => rdd.map(x=>x._2).foreach(x=>{val y = x.getTotalSize ; println("Size"+y)}))
    /*val TotalSize = lines.map(_._2).map{stream => stream.getTotalSize }
    TotalSize.print()*/
    val header = lines.map(_._2).map {
    packet => if (packet.hasHeader(tcp)) { if (packet.hasHeader(ip)) {

      val protocol = "TCP"
      val ReceivedTime = new Date(packet.getCaptureHeader.timestampInMillis());
      val sourceip = FormatUtils.ip(ip.source())
      val destinationip = FormatUtils.ip(ip.destination())
     
      println(protocol + "\t" + ReceivedTime + "\t" + sourceip + "\t" + destinationip + "\t" + tcp.source() + "\t" + tcp.destination())
      outfile.append("\n"+protocol+"\t"+packet.getTotalSize+"\t"+ReceivedTime+"\t"+sourceip+"\t"+destinationip+"\t"+tcp.source()+"\t"+tcp.destination())
    }}


  else if (packet.hasHeader(udp)) { if (packet.hasHeader(ip)) {

      val protocol = "UDP"
      val ReceivedTime = new Date(packet.getCaptureHeader.timestampInMillis());
      val sourceip = FormatUtils.ip(ip.source())
      val destinationip = FormatUtils.ip(ip.destination())
      println(protocol + "\t" + ReceivedTime + "\t" + sourceip + "\t" + destinationip + "\t" + udp.source() + "\t" + udp.destination())
      outfile.append("\n"+protocol+"\t"+packet.getTotalSize+"\t"+ReceivedTime+"\t"+sourceip+"\t"+destinationip+"\t"+udp.source()+"\t"+udp.destination())

  }}
  /*else if (packet.hasHeader(ip) && packet.hasHeader(icmp)) {

      val protocol = "ICMP"
      val ReceivedTime = new Date(packet.getCaptureHeader.timestampInMillis())
      val sourceip = FormatUtils.ip(ip.source())
      val destinationip = FormatUtils.ip(ip.destination())
      println(protocol + "\t" + ReceivedTime + "\t" + sourceip + "\t" + destinationip)
      outfile.append("\n"+protocol+"\t"+packet.getTotalSize+"\t"+ReceivedTime+"\t"+sourceip+"\t"+destinationip)
    }*/
    else {
    lines.print()
  }

  }
   header.print()
     /* val header = words.map{stream => stream.getCaptureHeader.wirelen()}
    header.print()*/


    ssc.start()
    ssc.awaitTermination()
  }
}

