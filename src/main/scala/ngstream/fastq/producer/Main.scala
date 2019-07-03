package ngstream.fastq.producer

import java.io.File
import java.util.Properties

import ngstream.schema.{FastqData, FastqDataSerde, ReadId, ReadIdSerde}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scopt.{OParser, OParserBuilder}

object Main {
  def main(args: Array[String]): Unit = {
    val cmdArgs = OParser.parse(argParser, args, CmdArgs())
      .getOrElse(throw new IllegalArgumentException())
    val reader = new Reader(cmdArgs.r1, cmdArgs.r2)

    val props = new Properties()
    props.put("bootstrap.servers", cmdArgs.brokers.mkString(","))
    props.put("key.serializer", classOf[ReadIdSerde].getCanonicalName)
    props.put("value.serializer", classOf[FastqDataSerde].getCanonicalName)
    val producer = new KafkaProducer[ReadId, FastqData](props)

    reader.foreach(record => {
      producer.send(new ProducerRecord(cmdArgs.topic, record.key, record.value))
    })
    reader.close()
    producer.flush()
    producer.close()
  }

  case class CmdArgs(r1: File = null,
                     r2: Option[File] = None,
                     brokers: Seq[String] = Seq(),
                     topic: String = null)

  def argParser: OParser[Unit, CmdArgs] = {
    val builder: OParserBuilder[CmdArgs] = OParser.builder[CmdArgs]
    import builder._
    OParser.sequence(
      head("Ngstream fastq kafka producer"),
      opt[File]("R1")
        .action((x, c) => c.copy(r1 = x))
        .required()
        .text("File of R1 records"),
      opt[File]("R2")
        .action((x, c) => c.copy(r2 = Some(x)))
        .text("File of R2 records"),
      opt[String]("broker")
        .action((x, c) => c.copy(brokers = c.brokers :+ x))
        .required()
        .text("Kafka broker, can supply multiple"),
      opt[String]("topic")
        .action((x, c) => c.copy(topic = x))
        .required()
        .text("Kafka topic")
    )
  }
}
