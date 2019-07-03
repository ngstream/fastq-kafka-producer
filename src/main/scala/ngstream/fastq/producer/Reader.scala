package ngstream.fastq.producer

import java.io.File

import htsjdk.samtools.fastq.FastqReader
import ngstream.schema.{Fastq, FastqData, FastqSequence, ReadId}

import scala.collection.JavaConverters.asScalaIterator

class Reader(r1File: File, r2File: Option[File]) extends Iterator[Fastq] with AutoCloseable {

  private val readerR1 = new FastqReader(r1File)
  private val readerR2 = r2File.map(new FastqReader(_))

  private val it: Iterator[Fastq] = readerR2 match {
    case Some(r) => pairedIt(r)
    case _ => singleIt()
  }

  private def singleIt(): Iterator[Fastq] = {
    asScalaIterator(readerR1)
      .map(x => Fastq(ReadId(0, x.getReadName), FastqData(Seq(FastqSequence(x.getReadBases, x.getBaseQualities)))))
  }

  private def pairedIt(r2It: FastqReader): Iterator[Fastq] = {
    asScalaIterator(readerR1).zip(asScalaIterator(r2It))
      .map { case (r1, r2) =>
        //TODO: name check
        Fastq(ReadId(0, r1.getReadName), FastqData(Seq(
          FastqSequence(r1.getReadBases, r1.getBaseQualities),
          FastqSequence(r2.getReadBases, r2.getBaseQualities))))
      }
  }

  def hasNext: Boolean = it.hasNext

  def next(): Fastq = it.next()

  def close(): Unit = {
    readerR1.close()
    readerR2.foreach(_.close())
  }
}
