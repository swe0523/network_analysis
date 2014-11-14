/**
 * Created by root on 11/9/14.
 */
import java.lang.StringBuilder
import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.jnetpcap.Pcap
import org.jnetpcap.packet.{PcapPacket, PcapPacketHandler}

object kafkaproducer extends Serializable{
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCountProducer <metadataBrokerList> <topic> " +
        "<messagesPerSec> <wordsPerMessage>")
      System.exit(1)
    }
    //metadata.broker.list=localhost:9092
    //zookeeper.connect=localhost:2181
    val Array(brokers, topic, messagesPerSec, wordsPerMessage) = args
    // Zookeeper connection properties
    val props = new Properties()
    props.put("metadata.broker.list", brokers.toString)
    props.put("serializer.class", "PcapEncoder")
    val config = new ProducerConfig(props)
    val producer = new Producer[String,PcapPacket](config)
    val file = "/home/Desktop/test.pcapng"
    // Send some messages
    val snaplen = 64 * 1024 // Capture all packets, no truncation
    val flags = Pcap.MODE_PROMISCUOUS // capture all packets
    val timeout = 10 * 1000
    val jsb = new java.lang.StringBuilder()
    val errbuf = new StringBuilder(jsb);
    //val pcap = Pcap.openLive("eth0", snaplen, flags, timeout, errbuf)
    val pcap = Pcap.openOffline(file,errbuf)
    if (pcap == null) {
      println("Error : " + errbuf.toString())
    }

    while(true){

      val jpacketHandler = new PcapPacketHandler[String]() {

        def nextPacket(packet: PcapPacket, user: String) {
         val data = new KeyedMessage[String,PcapPacket](topic.toString,(packet))
          producer.send(data)
        

        }
      }
      pcap.loop(50, jpacketHandler, "jNetPcap works!")


    }

  }
}
