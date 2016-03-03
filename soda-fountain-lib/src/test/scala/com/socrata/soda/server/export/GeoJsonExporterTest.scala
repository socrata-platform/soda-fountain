package com.socrata.soda.server.export

import javax.servlet.ServletOutputStream

import com.rojoma.simplearm.util._
import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.io.JsonReader
import com.rojoma.json.v3.conversions._
import com.socrata.soda.server.export.GeoJsonProcessor.InvalidGeoJsonSchema
import com.socrata.soda.server.id.ColumnId
import com.socrata.soda.server.persistence.ColumnRecord
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types._
import java.io.{OutputStream, ByteArrayOutputStream, StringWriter, PrintWriter}
import javax.servlet.http.HttpServletResponse
import org.joda.time.format.ISODateTimeFormat

class GeoJsonExporterTest  extends ExporterTest {
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

    val expectedGeoJson =
      JObject(Map(
        "type"     -> JString("FeatureCollection"),
        "features" -> JArray(Array(
          JObject(Map(
            "type"     -> JString("Feature"),
            "geometry" -> JObject(Map(
              "type" -> JString("Point"),
              "coordinates" -> JArray(Array(JNumber.unsafeFromString("-122.314822"),
                JNumber.unsafeFromString("47.630269"))))),
            "properties" -> JObject(Map("name" -> JString("Volunteer Park"))))),
          JObject(Map(
            "type"     -> JString("Feature"),
            "geometry" -> JObject(Map(
              "type" -> JString("Point"),
              "coordinates" -> JArray(Array(JNumber.unsafeFromString("-122.319071"),
                JNumber.unsafeFromString("47.617296"))))),
            "properties" -> JObject(Map("name" -> JString("Cal Anderson Park"))))),
          JObject(Map(
            "type"     -> JString("Feature"),
            "geometry" -> JObject(Map(
              "type" -> JString("Point"),
              "coordinates" -> JArray(Array(JNumber.unsafeFromString("-122.252513"),
                JNumber.unsafeFromString("47.55553"))))), // in v3, we don't keep trailing 0s.  Was 47.555530
            "properties" -> JObject(Map("name" -> JString("Seward Park")))))
        )),
        expectedProjection))

    geoJson should be (expectedGeoJson)
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
            "coordinates" -> JArray(Array(JNumber.unsafeFromString("-122.314822"),
              JNumber.unsafeFromString("47.630269"))))),
          "properties" -> JObject(Map("name" -> JString("Volunteer Park"))),
          expectedProjection)))
  }

  test("Single row - dataset with null geo column") {
    val columns = Seq(
      new ColumnRecord(ColumnId("hym8-ivsj"), ColumnName("name"), SoQLText, "name", "", false, None),
      new ColumnRecord(ColumnId("pw2s-k39x"), ColumnName("location"), SoQLPoint, "location", "", false, None)
    )

    val rows = Seq[Array[SoQLValue]](
      Array(SoQLText("Volunteer Park"), SoQLNull)
    )

    val geoJson = getGeoJson(columns, "hym8-ivsj", rows, true)
    geoJson should be (
        JObject(Map(
          "type"     -> JString("Feature"),
          "geometry" -> JNull,
          "properties" -> JObject(Map("name" -> JString("Volunteer Park"))),
          expectedProjection)))
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

    val geoJson = getGeoJson(columns, "hym8-ivsj", rows, false)
    geoJson should be (JObject(Map(
      "type"     -> JString("FeatureCollection"),
      "features" -> JArray(Array(
        JObject(Map(
          "type"     -> JString("Feature"),
          "geometry" -> JObject(Map(
            "type" -> JString("Point"),
            "coordinates" -> JArray(Array(JNumber.unsafeFromString("-122.314822"),
              JNumber.unsafeFromString("47.630269"))))),
          "properties" -> JObject(Map("name" -> JString("Volunteer Park"))))),
        JObject(Map(
          "type"     -> JString("Feature"),
          "geometry" -> JObject(Map(
            "type" -> JString("Point"),
            "coordinates" -> JArray(Array(JNumber.unsafeFromString("-122.319071"),
              JNumber.unsafeFromString("47.617296"))))),
          "properties" -> JObject(Map("name" -> JString("Cal Anderson Park"))))),
        JObject(Map(
          "type"     -> JString("Feature"),
          "geometry" -> JNull,
          "properties" -> JObject(Map("name" -> JString("Phantom Park"))))),
        JObject(Map(
          "type"     -> JString("Feature"),
          "geometry" -> JObject(Map(
            "type" -> JString("Point"),
            "coordinates" -> JArray(Array(JNumber.unsafeFromString("-122.252513"),
              JNumber.unsafeFromString("47.55553"))))),
          "properties" -> JObject(Map("name" -> JString("Seward Park")))))
      )),
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

  private class FakeServletOutputStream(underlying: OutputStream) extends ServletOutputStream {
    override def write(b: Int): Unit = underlying.write(b)
    override def write(bs: Array[Byte]): Unit = underlying.write(bs)
    override def write(bs: Array[Byte], offset: Int, length: Int): Unit = underlying.write(bs, offset, length)
  }

  private def getGeoJson(columns: Seq[ColumnRecord],
                         pk: String,
                         rows: Seq[Array[SoQLValue]],
                         singleRow: Boolean): JValue = {
    for {
      out <- managed(new ByteArrayOutputStream)
      wrapped <- managed(new FakeServletOutputStream(out))
    } yield {
      val mockResponse = mock[HttpServletResponse]
      mockResponse.expects('setContentType)("application/vnd.geo+json; charset=UTF-8")
      mockResponse.expects('getOutputStream)().returning(wrapped)

      GeoJsonExporter.export(charset, getDCSchema("GeoJsonExporterTest", columns, pk, rows), rows.iterator, singleRow)(mockResponse)

      JsonReader.fromString(new String(out.toByteArray, "UTF-8"))
    }
  }

}
