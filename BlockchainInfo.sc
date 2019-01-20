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
def downloadHeadersForDate(year: Int, month: Int, day: Int): List[ChainHeader] = {
   val dtIso = s"${year}-${pad2(month)}-${pad2(day)}T00:00:00.0Z"
   val tm = java.sql.Timestamp.from(java.time.Instant.parse(dtIso)).getTime
   val url = s"https://www.blockchain.com/btc/blocks/${tm}"

   import scala.collection.JavaConverters._

   val doc: org.jsoup.nodes.Document = org.jsoup.Jsoup.connect(url).get
   val table: org.jsoup.nodes.Element = doc.select("table.table.table-striped").first

   val res = for {
       tr <- table.select("tr").asScala
       a = tr.select("a").eachText
       if a.size > 0
   } yield ChainHeader(a.get(0).toInt, a.get(1))
   res.toList
}

def getHeadersForDate(year: Int, month: Int, day: Int): List[ChainHeader] = {
    val sCacheFile = s"./data/${year}-${pad2(month)}-${pad2(day)}.json"
    val file = new java.io.File(sCacheFile)
    if (file.exists) {
        val input = scala.io.Source.fromFile(sCacheFile).getLines.mkString
        parser.decode[List[ChainHeader]](input) match {
            case Right(list) => 
                list
            case Left(error) => {
                println("ERROR:" + error); 
                List()
            }
        }
    } else {
        import java.io.PrintWriter
        val headers = downloadHeadersForDate(year, month, day)
        new PrintWriter(file) { write(headers.asJson.toString); close }
        headers
    }
}

import java.time.{ Month, LocalDate, Instant }
val year = LocalDate.now.getYear
def between(fromDate: LocalDate, toDate: LocalDate) = fromDate.toEpochDay.until(toDate.toEpochDay).map(LocalDate.ofEpochDay)
val dateRange = between(LocalDate.of(year, Month.JANUARY, 1), LocalDate.now).reverse
val sum = dateRange.map { dt =>
    val hashes = getHeadersForDate(dt.getYear, dt.getMonth.getValue, dt.getDayOfMonth)
    val minHeight = hashes.map(_.height).min
    val maxHeight = hashes.map(_.height).max
    println(dt, hashes.length, minHeight, maxHeight)
    hashes.length
}.reduce(_+_)
println("total: " + sum + " blocks")