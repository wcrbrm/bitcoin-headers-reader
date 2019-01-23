import $ivy.`org.jsoup:jsoup:1.11.3`
import $ivy.`io.circe::circe-core:0.10.0`
import $ivy.`io.circe::circe-parser:0.10.0`
import $ivy.`io.circe::circe-generic:0.10.0`

import io.circe._
import io.circe.parser._
import io.circe.syntax._
import io.circe.generic.semiauto._

case class ChainHeader(height: Int, hash: String)
object ChainHeader {
   implicit val chainHeaderDecoder: Decoder[ChainHeader] = deriveDecoder[ChainHeader]
   implicit val chainHeaderEncoder: Encoder[ChainHeader] = deriveEncoder[ChainHeader]
}
import ChainHeader._

def pad2(x: Int): String = if(x >= 10) x.toString else s"0${x}"
def getHeadersForDate(year: Int, month: Int, day: Int): List[ChainHeader] = {
    val sCacheFile = s"./data/${year}-${pad2(month)}-${pad2(day)}.json"
    val file = new java.io.File(sCacheFile)
    if (file.exists) {
        val input = scala.io.Source.fromFile(sCacheFile).getLines.mkString
        parser.decode[List[ChainHeader]](input) match {
            case Right(list) => 
                list
            case Left(error) => {
                throw new Exception("ERROR:" + error); 
            }
        }
    } else {
        throw new Exception("No headers for date")
    }
}

def niceTime(tm: Long) = {
    // val ts = new java.sql.Timestamp(tm)
    // val date = new java.util.Date(tm)
    val ts  = tm * 1000L
    val df = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    tm + ": " + df.format(ts)
}

import java.time.{ Month, LocalDate, Instant }
val year = LocalDate.now.getYear - 2
def between(fromDate: LocalDate, toDate: LocalDate) = fromDate.toEpochDay.until(toDate.toEpochDay).map(LocalDate.ofEpochDay)
val dateRange = between(
    LocalDate.of(year, Month.JANUARY, 1), 
    LocalDate.of(2019, Month.JANUARY, 18)
).reverse

var blocksList = dateRange.map { dt =>
    getHeadersForDate(dt.getYear, dt.getMonth.getValue, dt.getDayOfMonth)
}.flatten.sortWith(_.height < _.height)

import java.io.{ File, FileInputStream }
import java.util.zip.GZIPInputStream

object Gzip {
  def decompress(fIn: File): Option[String] =
    scala.util.Try {
      val inputStream = new GZIPInputStream(new FileInputStream(fIn))
      scala.io.Source.fromInputStream(inputStream).mkString
    }.toOption
}

val folderAmounts: String = scala.util.Properties.envOrElse("HASHES_FOLDER", "/tmp/cache/btc/amounts")
def getHashDir(hash: String): String = {
  new StringBuffer(folderAmounts).append("/")
    .append(hash.substring( hash.length - 3 ) ).toString
}
def getHashFile(hash: String): File = {
  val sb = new StringBuffer(getHashDir(hash)).append( "/" ).append(hash).append(".json.gz").toString
  new File(sb)
}

case class AmountsPacket(packet: String, time: Long, hash: String, prev: String, values: Map[String, Int])
implicit val amountsPacketDecoder: Decoder[AmountsPacket] = deriveDecoder[AmountsPacket]
implicit val amountsPacketEncoder: Encoder[AmountsPacket] = deriveEncoder[AmountsPacket]

println(blocksList.size + " blocks in the chain")
var errors = 0
blocksList.zipWithIndex.foreach{ 
  case (h: ChainHeader, index: Int) =>
    val file = getHashFile(h.hash)
    val isFirst = h.height == blocksList(0).height
    val isLast = h.height == blocksList(blocksList.size - 1).height
    val prevHash: String = if (isFirst) "" else blocksList(index - 1).hash
    val thisHash: String = blocksList(index).hash   
    // println(h.height + "\t" + h.hash + " " + prevHash)

    if (!file.exists) {
        println("height=" + h.height + ", file is missing " + file.getAbsolutePath)
        errors += 1
    } else {
        Gzip.decompress(file) match {
            case Some(contents) => {
                decode[AmountsPacket](contents) match {
                    case Left(err) => {
                        println("height=" + h.height + ", invalid json " + err)        
                        errors += 1
                    }
                    case Right(amounts) => {
                        if( amounts.hash != h.hash) {
                            println("height=" + h.height + ", unexpected hash, get " + amounts.hash + ", expected " + h.hash)        
                            errors += 1
                        } else if (prevHash != "" && amounts.prev != prevHash) {
                            println("height=" + h.height + ", time= " + niceTime(amounts.time) + ", prev hash  mismatch " + amounts.prev + ", expected " + prevHash + " in " + file.getAbsolutePath)        
                            errors += 1
                        } else {
                            if (index % 1000 == 0) println("...height=" + h.height)
                        }
                    }
                }
            }
            case None => {
                println("height=" + h.height + ", file is not decoded correctly " + file.getAbsolutePath)
                errors += 1
            }
        }
        
    }
}

println(blocksList.head.height + " ... " + blocksList(blocksList.size - 1).height)
println(errors + " integrity errors found")


