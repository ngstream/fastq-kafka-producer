name := "fastq-kafka-producer"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies += "com.sksamuel.avro4s" %% "avro4s-kafka" % "2.0.4"
libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.0-RC2"
libraryDependencies += "com.github.samtools" % "htsjdk" % "2.11.0"
libraryDependencies += "org.apache.kafka" %% "kafka" % "2.2.0"
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.26"
