name := "kafkapublisher"

version := "1.0"

scalaVersion := "2.10.3"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.0.2"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.0.2"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.0.2"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"