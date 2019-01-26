import $ivy.`com.typesafe.akka::akka-actor:2.5.18`
import $ivy.`com.typesafe.akka::akka-stream:2.5.18`
import $ivy.`com.lightbend.akka::akka-stream-alpakka-file:1.0-M2`
import $ivy.`io.circe::circe-core:0.10.0`
import $ivy.`io.circe::circe-parser:0.10.0`
import $ivy.`io.circe::circe-generic:0.10.0`

import scala.util.Properties
import io.circe._
import io.circe.parser._
import io.circe.syntax._
import io.circe.generic.semiauto._
import java.io.{ File, FileInputStream }
import java.util.zip.GZIPInputStream

object Gzip {
  def decompress(fIn: File): Option[String] =
    scala.util.Try {
      val inputStream = new GZIPInputStream(new FileInputStream(fIn))
      scala.io.Source.fromInputStream(inputStream).mkString
    }.toOption
}

case class AmountsPacket(packet: String, time: Long, hash: String, prev: String, values: Map[String, Int])
implicit val amountsPacketDecoder: Decoder[AmountsPacket] = deriveDecoder[AmountsPacket]
implicit val amountsPacketEncoder: Encoder[AmountsPacket] = deriveEncoder[AmountsPacket]


import akka.stream.alpakka.file.scaladsl.Directory
import java.nio.file.{ Paths, FileVisitOption }
import scala.collection.immutable.StringOps
import akka.stream.scaladsl._
import scala.concurrent.{ Future, Await }
import scala.concurrent.duration._

implicit val system = akka.actor.ActorSystem("btc-amounts")
implicit val mat = akka.stream.ActorMaterializer()
implicit val ec = system.dispatcher

var errors = 0
def handlePacket(packet: AmountsPacket): Unit = {
    packet.values.keys.map(value => {
        if (
            value.toString.length > 10 && 
            value.toLong >= 55500000000L
        ) {
            val v = new StringOps(value).reverse.padTo(14, "-").reverse.mkString("")
            val withDots = v.substring(0, v.length - 8) + "." + v.substring(v.length - 8)
            println("BLOCK\t" + packet.hash + " " + withDots )
        }
    })
}

val folderAmounts: String = Properties.envOrElse("HASHES_FOLDER", "/tmp/cache/btc/amounts")
val parallelism = Runtime.getRuntime.availableProcessors
val root = Paths.get( folderAmounts )
println("starting...")
val done = Directory.walk(root, maxDepth = Some(2), List(FileVisitOption.FOLLOW_LINKS))
    .filter(_.toString.endsWith(".gz"))
    .map(_.toString)
    .take(1000)
    .mapAsyncUnordered(parallelism){ file => 
        Gzip.decompress(new File(file)) match {
            case Some(contents) => {
                decode[AmountsPacket](contents) match {
                    case Left(err) => {
                        println("ERROR: Invalid json " + err)        
                        errors += 1
                    }
                    case Right(amounts) => handlePacket(amounts)
                }
            }
            case None => {
                println("ERROR: file is not decompressed correctly " + file)
                errors += 1
            }
        }
        Future { 0 }
    }.runWith(Sink.seq)

done.onComplete{ _ => println("COMPLETE") }
