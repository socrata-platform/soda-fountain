package com.socrata.soda.server.highlevel

import org.scalatest.{Assertions, FunSuite}
import org.scalatest.matchers.MustMatchers
import com.socrata.soql.types._
import com.socrata.soql.environment.ColumnName
import com.socrata.soda.server.id.ColumnId
import com.socrata.soda.server.persistence.MinimalColumnRecord
import com.rojoma.json.ast._
import com.rojoma.json.ast.JString
import com.rojoma.json.io.JsonReader
import com.typesafe.config.ConfigFactory

class GeospaceHandlerTest extends FunSuite with MustMatchers with Assertions {
  import collection.JavaConverters._
  import ComputationHandler._

  val testConfig = ConfigFactory.parseMap(Map(
                     "fake-coder" -> true,
                     "batch-size" -> 2
                   ).asJava)

  val point1 = "{\"type\":\"Point\",\"coordinates\":[47.6303,-122.3148]}"
  val point2 = "{\"type\":\"Point\",\"coordinates\":[48.6303,-121.3148]}"
  val point3 = "{\"type\":\"Point\",\"coordinates\":[49.6303,-120.3148]}"
  val point4 = "{\"type\":\"Point\",\"coordinates\":[50.11,  -119.98]}"
  val multiLine = """{"type":"MultiLineString","coordinates":[[[100,0.123456789012],[101,1]],[[102,2],[103,3]]]}"""

  val testRows = Seq(
                   Map("geom" -> JsonReader.fromString(point1), "date" -> JString("12/31/2013")),
                   Map("geom" -> JsonReader.fromString(point2), "date" -> JString("11/30/2013")),
                   Map("geom" -> JsonReader.fromString(point3), "date" -> JString("12/4/2013")),
                   Map("geom" -> JsonReader.fromString(point4), "date" -> JString("1/14/2014"))
                 ).map(JObject(_))

  val handler = new GeospaceHandler(testConfig)
  val columnSpec = MinimalColumnRecord(ColumnId("foo"), ColumnName("ward_id"), SoQLText, false)

  test("Can compute feature IDs with fake coder with valid input") {
    val expectedIds = Seq("47.6303/-122.3148", "48.6303/-121.3148", "49.6303/-120.3148", "50.11/-119.98")
    val expectedRows = testRows.zip(expectedIds).map { case (JObject(map), id) =>
      JObject(map + ("ward_id" -> JString(id)))
    }
    val newRows = handler.compute(testRows.toIterator, columnSpec)
    newRows.toSeq must equal (expectedRows)
  }

  test("Will throw UnknownColumnEx if source column missing") {
    intercept[UnknownColumnEx] {
      val rows = Seq(JObject(Map("date" -> JString("12/31/2013"))))
      // NOTE: must call next on an iterator otherwise computation doesn't start
      handler.compute(rows.toIterator, columnSpec).next
    }
  }

  test("handler.compute() returns lazy iterator") {
    // The way we verify this is a variant of above test.  Unless we call next(), errors in the input
    // will not result in an exception because processing hasn't started yet
    val rows = Seq(JObject(Map("date" -> JString("12/31/2013"))))    // geom column missing
    handler.compute(rows.toIterator, columnSpec)
  }

  test("Will throw MaltypedDataEx if source column not in right format") {
    // If not GeoJSON but other valid JSON
    intercept[MaltypedDataEx] {
      val rows = Seq(JObject(Map("geom" -> JArray(Seq(JNumber(1), JString("two"))))))
      handler.compute(rows.toIterator, columnSpec).next
    }

    // If GeoJSON but not Point
    intercept[MaltypedDataEx] {
      val rows = Seq(JObject(Map("geom" -> JsonReader.fromString(multiLine))))
      handler.compute(rows.toIterator, columnSpec).next
    }

  }
}