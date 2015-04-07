package com.socrata.soda.server.export

import java.io.{DataInputStream, FileInputStream}
import org.velvia.MsgPack
import org.velvia.MsgPackUtils._
import com.vividsolutions.jts.io.WKBReader

/**
 * App that takes a SoQLPack stream from STDIN and dumps out the header and rows
 * This should be moved into part of the main instead of test package, but this would
 * mess up the java -jar soda-fountain-assembly experience...
 *
 * Run in the soda-fountain directory like:
 *  curl ... | sbt 'soda-fountain-lib/test:runMain com.socrata.soda.server.export.SoQLPackDumper'
 */
object SoQLPackDumper extends App {
  val dis = new DataInputStream(System.in)
  val headerMap = MsgPack.unpack(dis, MsgPack.UNPACK_RAW_AS_STRING).asInstanceOf[Map[String, Any]]
  println("--- Schema ---")
  headerMap.foreach { case (key, value) => println("%25s: %s".format(key, value)) }
  val types = headerMap.as[Seq[Map[String, String]]]("schema").map { m => m("t") }

  println("\n---")

  val reader = new WKBReader

  while (dis.available > 0) {
    // Don't unpack raw byte arrays as Strings - they might be Geometries or other blobs
    val row = MsgPack.unpack(dis, 0).asInstanceOf[Seq[Any]]
    for (i <- 0 until row.length) {
      if (Set("point", "multiline", "multipolygon") contains types(i)) {
        print(reader.read(row(i).asInstanceOf[Array[Byte]]))
      } else if (types(i) == "text") {
        print(new String(row(i).asInstanceOf[Array[Byte]]))
      } else {
        print(row(i).toString)
      }
      print(", ")
    }
    println
  }

}