package com.socrata.soda.server.export

import java.io.{DataOutputStream, OutputStream}

import com.rojoma.simplearm.util._
import com.socrata.soql.SoQLPackEncoder
import com.socrata.soql.types.{SoQLNull, SoQLValue, SoQLGeometryLike, SoQLType}
import com.vividsolutions.jts.io.WKBWriter
import org.velvia.MsgPack

class SoQLPackWriter(schema: Seq[(String, SoQLType)],
                     extraHeaders: Map[String, Any] = Map.empty) {

  val geoColumn = getGeoColumnIndex(schema.map(_._2))

  private def getGeoColumnIndex(columns: Seq[SoQLType]): Int = {
    val geoColumnIndices = columns.zipWithIndex.collect {
      case (typ, index) if typ.isInstanceOf[SoQLGeometryLike[_]] => index
    }

    // Just return -1 if no geo column or more than one
    if (geoColumnIndices.size != 1) return -1

    geoColumnIndices(0)
  }

  val nullEncoder: PartialFunction[SoQLValue, Any] = {
    case SoQLNull            => null
    case null                => null
  }

  val encoders = schema.map { case (colName, colType) =>
    SoQLPackEncoder.encoderByType(colType) orElse nullEncoder
  }

  /**
   * Serializes the rows into SoQLPack binary format, writing it out to the ostream.
   * The caller must be responsible for closing the output stream.
   */
  def write(ostream: OutputStream, rows: Iterator[Exporter#ArrayLike[SoQLValue]]) {
    for {
      dos <- managed(new DataOutputStream(ostream))
    } yield {
      val schemaMaps = schema.map { case (name, typ) =>
        Map("c" -> name, "t" -> typ.toString)
      }.toSeq

      val headerMap: Map[String, Any] = Map("schema" -> schemaMaps,
        "geometry_index" -> geoColumn) ++ extraHeaders
      MsgPack.pack(headerMap, dos)

      // end of header

      val wkbWriter = new WKBWriter

      for (row <- rows) {
        val values: Seq[Any] = (0 until row.length).map { i =>
          encoders(i)(row(i))
        }
        MsgPack.pack(values, dos)
      }
      dos.flush()
    }
  }
}
