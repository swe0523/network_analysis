import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.jnetpcap.packet.PcapPacket


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

    val Array(zkQuorum, group, topics, numThreads) = args
    val kafkaParams = Map[String, String]("zookeeper.connect" -> zkQuorum, "group.id" -> group)
    val sparkConf = new SparkConf().setMaster("local").setAppName("kafkaconsumer")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")
    val topicpMap = topics.split(",").map((_,numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream[String, PcapPacket, StringDecoder, PcapDecoder](ssc, kafkaParams, Map(topics -> 1), StorageLevel.MEMORY_ONLY)
   // val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicpMap) //[String,String,StringDecoder,StringDecoder].toString()
    lines.foreachRDD(rdd=>rdd.map(x=>println(x._2)))
    lines.print()

   /* val words = lines.map(_._2)
    words.print()
     val header = words.map{stream => stream.getCaptureHeader().wirelen()}
    header.print()*/


    ssc.start()
    ssc.awaitTermination()
  }
}

