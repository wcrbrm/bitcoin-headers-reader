import $ivy.`org.jsoup:jsoup:1.11.3`

def downloadHeadersForDate(year: Int, month: Int, day: Int): List[(Int, String)] = {
   def pad2(x: Int): String = if(x >= 10) x.toString else s"0${x}"
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
   } yield (a.get(0).toInt, a.get(1))
   res.toList
}


import java.time.{ Month, LocalDate, Instant }

val year = LocalDate.now.getYear
def between(fromDate: LocalDate, toDate: LocalDate) = fromDate.toEpochDay.until(toDate.toEpochDay).map(LocalDate.ofEpochDay)
val dateRange = between(LocalDate.of(year, Month.JANUARY, 1), LocalDate.now).reverse
dateRange.map { dt =>
    val startTime = Instant.now
    val hashes = downloadHeadersForDate(dt.getYear, dt.getMonth.getValue, dt.getDayOfMonth)
    println(startTime, dt, hashes.length)
}
