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

implicit val system = akka.actor.ActorSystem("btc-amounts")
implicit val mat = akka.stream.ActorMaterializer()

var errors = 0
def handlePacket(packet: AmountsPacket): Unit = {
    packet.values.keys.map(value => {
        if (value.toString.length > 10 && 
            value.toString.contains("66666666666")) {
            val v = new StringOps(value).padTo(10, "-").mkString("")
            println("BLOCK\t" + packet.hash + "\t" + v )
        }
    })
}

val folderAmounts: String = Properties.envOrElse("HASHES_FOLDER", "/tmp/cache/btc/amounts")
val root = Paths.get( folderAmounts )
println("starting...")
Directory.walk(root, maxDepth = Some(2), List(FileVisitOption.FOLLOW_LINKS))
    .filter(_.toString.endsWith(".gz"))
    .map(_.toString)
    .take(10000000)
    .runForeach{ file => 
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
    }

// scala.io.StdIn.readLine
