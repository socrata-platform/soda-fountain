package com.socrata.soda.server.export

import com.rojoma.simplearm.util._
import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.io.JsonReader
import com.socrata.http.common.util.AliasedCharset
import com.socrata.soda.server.DatasetsForTesting
import com.socrata.soda.server.highlevel.{ExportDAO, CJson}
import com.socrata.soda.server.highlevel.ExportDAO.ColumnInfo
import com.socrata.soda.server.id.ColumnId
import com.socrata.soda.server.persistence.ColumnRecord
import com.socrata.soda.server.wiremodels.JsonColumnRep
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types._
import java.io.{ByteArrayOutputStream, ByteArrayInputStream, DataInputStream}
import java.nio.charset.StandardCharsets
import javax.servlet.ServletOutputStream
import javax.servlet.http.HttpServletResponse
import org.joda.time.format.ISODateTimeFormat
import org.scalamock.proxy.ProxyMockFactory
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSuite, Matchers}
import org.velvia.MsgPack
import org.velvia.MsgPackUtils._

class SoQLPackExporterTest  extends FunSuite with MockFactory with ProxyMockFactory with Matchers with DatasetsForTesting {
  val charset = AliasedCharset(StandardCharsets.UTF_8, StandardCharsets.UTF_8.name)

  test("Multi row - dataset with single geo column") {
    val columns = Seq(
      new ColumnRecord(ColumnId("hym8-ivsj"), ColumnName("name"), SoQLText, "name", "", false, None),
      new ColumnRecord(ColumnId("pw2s-k39x"), ColumnName("location"), SoQLPoint, "location", "", false, None)
    )

    val rows = Seq[Array[SoQLValue]](
      Array(SoQLText("Volunteer Park"), SoQLPoint(SoQLPoint.WktRep.unapply("POINT (-122.314822 47.630269)").get)),
      Array(SoQLText("Cal Anderson Park"), SoQLPoint(SoQLPoint.WktRep.unapply("POINT (-122.319071 47.617296)").get)),
      Array(SoQLText("Seward Park"), SoQLPoint(SoQLPoint.WktRep.unapply("POINT (-122.252513 47.555530)").get))
    )

    val (header, outRows) = getSoQLPack(columns, "hym8-ivsj", rows, false)

    header.asInt("geometry_index") should be (1)
    outRows should have length (3)
    getStringFrom(outRows(2), 0) should equal ("Seward Park")
    getXYFromRowGeom(outRows(0), 1) should equal ((-122.314822, 47.630269))
  }

  test("Single row - dataset with single geo column") {
    val columns = Seq(
      new ColumnRecord(ColumnId("hym8-ivsj"), ColumnName("name"), SoQLText, "name", "", false, None),
      new ColumnRecord(ColumnId("pw2s-k39x"), ColumnName("location"), SoQLPoint, "location", "", false, None)
    )

    val rows = Seq[Array[SoQLValue]](
      Array(SoQLText("Volunteer Park"), SoQLPoint(SoQLPoint.WktRep.unapply("POINT (-122.314822 47.630269)").get))
    )

    val (header, outRows) = getSoQLPack(columns, "hym8-ivsj", rows, true)
    header.asInt("geometry_index") should be (1)
    outRows should have length (1)
    getStringFrom(outRows(0), 0) should equal ("Volunteer Park")
    getXYFromRowGeom(outRows(0), 1) should equal ((-122.314822, 47.630269))
  }

  test("Single row - dataset with null geo column") {
    val columns = Seq(
      new ColumnRecord(ColumnId("hym8-ivsj"), ColumnName("name"), SoQLText, "name", "", false, None),
      new ColumnRecord(ColumnId("pw2s-k39x"), ColumnName("location"), SoQLPoint, "location", "", false, None)
    )

    val rows = Seq[Array[SoQLValue]](
      Array(SoQLText("Volunteer Park"), SoQLNull)
    )

    val (header, outRows) = getSoQLPack(columns, "hym8-ivsj", rows, true)
    header.asInt("geometry_index") should be (1)
    outRows should have length (1)
    Option(outRows(0)(1)) should be (None)
  }


  test("Multi row - dataset with single geo column and some rows with empty geo value") {
    val columns = Seq(
      new ColumnRecord(ColumnId("hym8-ivsj"), ColumnName("name"), SoQLText, "name", "", false, None),
      new ColumnRecord(ColumnId("pw2s-k39x"), ColumnName("location"), SoQLPoint, "location", "", false, None)
    )

    val rows = Seq[Array[SoQLValue]](
      Array(SoQLText("Volunteer Park"), SoQLPoint(SoQLPoint.WktRep.unapply("POINT (-122.314822 47.630269)").get)),
      Array(SoQLText("Cal Anderson Park"), SoQLPoint(SoQLPoint.WktRep.unapply("POINT (-122.319071 47.617296)").get)),
      Array(SoQLText("Phantom Park"), null),
      Array(SoQLText("Seward Park"), SoQLPoint(SoQLPoint.WktRep.unapply("POINT (-122.252513 47.555530)").get))
    )

    val (header, outRows) = getSoQLPack(columns, "hym8-ivsj", rows, false)

    header.asInt("geometry_index") should be (1)
    outRows should have length (4)
    getStringFrom(outRows(3), 0) should equal ("Seward Park")
    getXYFromRowGeom(outRows(0), 1) should equal ((-122.314822, 47.630269))
    Option(outRows(2)(1)) should be (None)
  }

  private def getXYFromRowGeom(row: Seq[Any], index: Int): (Double, Double) = {
    val bytes = row(index).asInstanceOf[Array[Byte]]
    val pt = SoQLPoint.WkbRep.unapply(bytes).get
    (pt.getX, pt.getY)
  }

  private def getStringFrom(row: Seq[Any], index: Int): String =
    new String(row(index).asInstanceOf[Array[Byte]])

  private def getSoQLPack(columns: Seq[ColumnRecord],
                         pk: String,
                         rows: Seq[Array[SoQLValue]],
                         singleRow: Boolean): (Map[String, Any], Seq[Seq[Any]]) = {
    for { baos   <- managed(new ByteArrayOutputStream)
          stream <- managed(new ServletOutputStream { def write(b: Int) { baos.write(b) }} ) } yield {
      val mockResponse = mock[HttpServletResponse]
      mockResponse.expects('setContentType)("application/octet-stream")
      mockResponse.expects('getOutputStream)().returning(stream)

      SoQLPackExporter.export(mockResponse, charset, getDCSchema(columns, pk, rows), rows.iterator, singleRow)

      val dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray))
      val headerMap = MsgPack.unpack(dis, MsgPack.UNPACK_RAW_AS_STRING).asInstanceOf[Map[String, Any]]
      val outRows = new collection.mutable.ArrayBuffer[Seq[Any]]
      while (dis.available > 0) {
        outRows += MsgPack.unpack(dis, 0).asInstanceOf[Seq[Any]]
      }
      (headerMap, outRows)
    }
  }

  // TODO: extract these functions and the ones in GeoJsonExporterTest into common ExporterTest trait
  private def getDCSchema(columns: Seq[ColumnRecord], pk: String, rows: Seq[Array[SoQLValue]]): ExportDAO.CSchema = {
    val dcInfo = Iterator.single[JValue](getDCSummary(columns, pk, rows.size)) ++ getDCRows(rows)
    val decoded: CJson.Schema = CJson.decode(dcInfo) match {
      case CJson.Decoded(schema, rows) => schema
      case _ => fail("Something got messed up here")
    }

    val dataset = generateDataset("GeoJsonExporterTest", columns)

    ExportDAO.CSchema(decoded.approximateRowCount,
      decoded.dataVersion,
      decoded.lastModified.map(time => ISODateTimeFormat.dateTimeParser.parseDateTime(time)),
      decoded.locale,
      decoded.pk.map(dataset.columnsById(_).fieldName),
      decoded.rowCount,
      decoded.schema.map {
        f => ColumnInfo(dataset.columnsById(f.c).id, dataset.columnsById(f.c).fieldName, dataset.columnsById(f.c).name, f.t)
      })
  }

  private def getDCRows(rows: Seq[Array[SoQLValue]]) = {
    val jValues = rows.map(_.map { cell =>
      JsonColumnRep.forDataCoordinatorType(cell.typ).toJValue(cell)
    })
    jValues.map(JArray(_))
  }

  private def getDCSummary(columns: Seq[ColumnRecord], pk: String, rowCount: Int) = {
    val dcRowSchema = columns.map { column =>
      JObject(Map("c" -> JString(column.id.underlying), "t" -> JString(getHumanReadableTypeName(column.typ))))
    }
    JObject(Map("approximate_row_count" -> JNumber(rowCount),
                "locale"                -> JString("en_US"),
                "pk"                    -> JString(pk),
                "schema"                -> JArray(dcRowSchema)))
  }

  private def getHumanReadableTypeName(typ: SoQLType) = SoQLType.typesByName.map(_.swap).get(typ).get.name
}
