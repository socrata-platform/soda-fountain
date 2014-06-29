package com.socrata.soda.server.export

import com.rojoma.simplearm.util._
import com.rojoma.json.ast._
import com.rojoma.json.io.JsonReader
import com.socrata.http.common.util.AliasedCharset
import com.socrata.soda.server.DatasetsForTesting
import com.socrata.soda.server.export.GeoJsonExporter.InvalidGeoJsonSchema
import com.socrata.soda.server.highlevel.{ExportDAO, CJson}
import com.socrata.soda.server.highlevel.ExportDAO.ColumnInfo
import com.socrata.soda.server.id.ColumnId
import com.socrata.soda.server.persistence.ColumnRecord
import com.socrata.soda.server.wiremodels.JsonColumnRep
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types._
import java.io.{StringWriter, PrintWriter}
import java.nio.charset.StandardCharsets
import javax.servlet.http.HttpServletResponse
import org.joda.time.format.ISODateTimeFormat
import org.scalamock.proxy.ProxyMockFactory
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSuite, Matchers}

class GeoJsonExporterTest  extends FunSuite with MockFactory with ProxyMockFactory with Matchers with DatasetsForTesting {
  val charset = AliasedCharset(StandardCharsets.UTF_8, StandardCharsets.UTF_8.name)
  val expectedProjection = "crs" -> JObject(Map(
    "type" -> JString("name"),
    "properties" -> JObject(Map("name" -> JString("urn:ogc:def:crs:OGC:1.3:CRS84")))))

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

    val geoJson = getGeoJson(columns, "hym8-ivsj", rows, false)
    geoJson should be (JObject(Map(
      "type"     -> JString("FeatureCollection"),
      "features" -> JArray(Array(
        JObject(Map(
          "type"     -> JString("Feature"),
          "geometry" -> JObject(Map(
            "type" -> JString("Point"),
            "coordinates" -> JArray(Array(JNumber(-122.314822),
                                          JNumber(47.630269))))),
            "properties" -> JObject(Map("name" -> JString("Volunteer Park"))))),
        JObject(Map(
          "type"     -> JString("Feature"),
          "geometry" -> JObject(Map(
            "type" -> JString("Point"),
            "coordinates" -> JArray(Array(JNumber(-122.319071),
                                          JNumber(47.617296))))),
            "properties" -> JObject(Map("name" -> JString("Cal Anderson Park"))))),
        JObject(Map(
          "type"     -> JString("Feature"),
          "geometry" -> JObject(Map(
            "type" -> JString("Point"),
            "coordinates" -> JArray(Array(JNumber(-122.252513),
                                          JNumber(47.555530))))),
          "properties" -> JObject(Map("name" -> JString("Seward Park")))))
      )),
      expectedProjection)))
  }

  test("Single row - dataset with single geo column") {
    val columns = Seq(
      new ColumnRecord(ColumnId("hym8-ivsj"), ColumnName("name"), SoQLText, "name", "", false, None),
      new ColumnRecord(ColumnId("pw2s-k39x"), ColumnName("location"), SoQLPoint, "location", "", false, None)
    )

    val rows = Seq[Array[SoQLValue]](
      Array(SoQLText("Volunteer Park"), SoQLPoint(SoQLPoint.WktRep.unapply("POINT (-122.314822 47.630269)").get))
    )

    val geoJson = getGeoJson(columns, "hym8-ivsj", rows, true)
    geoJson should be (
        JObject(Map(
          "type"     -> JString("Feature"),
          "geometry" -> JObject(Map(
            "type" -> JString("Point"),
            "coordinates" -> JArray(Array(JNumber(-122.314822),
              JNumber(47.630269))))),
          "properties" -> JObject(Map("name" -> JString("Volunteer Park"))),
          expectedProjection)))
  }

  test("Dataset with no geo column") {
    val columns = Seq(
      new ColumnRecord(ColumnId("hym8-ivsj"), ColumnName("name"), SoQLText, "name", "", false, None),
      new ColumnRecord(ColumnId("pw2s-k39x"), ColumnName("date_constructed"), SoQLDate, "date_constructed", "", false, None)
    )

    val rows = Seq[Array[SoQLValue]](
      Array(SoQLText("Volunteer Park"), SoQLDate(ISODateTimeFormat.localDateParser.parseLocalDate("1900-01-01"))),
      Array(SoQLText("Cal Anderson Park"), SoQLDate(ISODateTimeFormat.localDateParser.parseLocalDate("1901-01-01"))),
      Array(SoQLText("Seward Park"), SoQLDate(ISODateTimeFormat.localDateParser.parseLocalDate("1902-01-01")))
    )

    a [InvalidGeoJsonSchema.type] should be thrownBy { getGeoJson(columns, "hym8-ivsj", rows, false) }
  }

  test("Dataset with multiple geo columns") {
    val columns = Seq(
      new ColumnRecord(ColumnId("hym8-ivsj"), ColumnName("name"), SoQLText, "name", "", false, None),
      new ColumnRecord(ColumnId("pw2s-k39x"), ColumnName("location1"), SoQLPoint, "location1", "", false, None),
      new ColumnRecord(ColumnId("dk3l-s2jk"), ColumnName("location2"), SoQLPoint, "location2", "", false, None)
    )

    val rows = Seq[Array[SoQLValue]](
      Array(SoQLText("Volunteer Park"),
            SoQLPoint(SoQLPoint.WktRep.unapply("POINT (-122.314822 47.630269)").get),
            SoQLPoint(SoQLPoint.WktRep.unapply("POINT (-124.314822 49.630269)").get))
    )

    a [InvalidGeoJsonSchema.type] should be thrownBy { getGeoJson(columns, "hym8-ivsj", rows, false) }
  }

  private def getGeoJson(columns: Seq[ColumnRecord],
                         pk: String,
                         rows: Seq[Array[SoQLValue]],
                         singleRow: Boolean): JValue = {
    for { out    <- managed(new StringWriter)
          writer <- managed(new PrintWriter(out)) } yield {
      val mockResponse = mock[HttpServletResponse]
      mockResponse.expects('setContentType)("application/vnd.geo+json; charset=UTF-8")
      mockResponse.expects('getWriter)().returning(writer)

      GeoJsonExporter.export(mockResponse, charset, getDCSchema(columns, pk, rows), rows.iterator, singleRow)

      JsonReader.fromString(out.toString)
    }
  }

  private def getDCSchema(columns: Seq[ColumnRecord], pk: String, rows: Seq[Array[SoQLValue]]): ExportDAO.CSchema = {
    val dcInfo = Iterator.single[JValue](getDCSummary(columns, pk, rows.size)) ++ getDCRows(rows)
    val decoded: CJson.Schema = CJson.decode(dcInfo) match {
      case CJson.Decoded(schema, rows) => schema
      case _ => fail("Something got messed up here")
    }

    val dataset = mockDataset("GeoJsonExporterTest", columns)

    ExportDAO.CSchema(decoded.approximateRowCount,
      decoded.dataVersion,
      decoded.lastModified.map(time => ISODateTimeFormat.dateTimeParser.parseDateTime(time)),
      decoded.locale,
      decoded.pk.map(dataset.columnsById(_).fieldName),
      decoded.rowCount,
      decoded.schema.map {
        f => ColumnInfo(dataset.columnsById(f.c).fieldName, dataset.columnsById(f.c).name, f.t)
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
