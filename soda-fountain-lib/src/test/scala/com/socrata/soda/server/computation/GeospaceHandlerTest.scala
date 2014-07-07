package com.socrata.soda.server.computation

import com.rojoma.json.ast._
import com.rojoma.json.io.JsonReader
import com.socrata.soda.server.highlevel.RowDataTranslator
import com.socrata.soda.server.highlevel.RowDataTranslator.{DeleteAsCJson, UpsertAsSoQL}
import com.socrata.soda.server.id.ColumnId
import com.socrata.soda.server.persistence.{MinimalColumnRecord, ComputationStrategyRecord}
import com.socrata.soda.server.wiremodels.{ComputationStrategyType, JsonColumnRep}
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types._
import com.typesafe.config.ConfigFactory
import org.mockserver.integration.ClientAndServer
import org.mockserver.model.Header
import org.scalatest.MustMatchers
import org.scalatest.{Assertions, FunSuite, BeforeAndAfterAll}

trait GeospaceHandlerData {
  val point1 = "{\"type\":\"Point\",\"coordinates\":[-122.3148,47.6303]}"
  val point2 = "{\"type\":\"Point\",\"coordinates\":[-121.3148,48.6303]}"
  val point3 = "{\"type\":\"Point\",\"coordinates\":[-120.3148,49.6303]}"
  val point4 = "{\"type\":\"Point\",\"coordinates\":[-119.98,50.11]}"
  val multiLine = """{"type":"MultiLineString","coordinates":[[[0.123456789012,100],[1,101]],[[2,102],[3,103]]]}"""

  val pointRep = JsonColumnRep.forClientType(SoQLPoint)
  def toSoQLPoint(str: String) = pointRep.fromJValue(JsonReader.fromString(str)).get.asInstanceOf[SoQLPoint]

  val testRows = Seq[RowDataTranslator.Success](
                   DeleteAsCJson(JString("abcd-1234")),
                   UpsertAsSoQL(Map("geom" -> toSoQLPoint(point1), "date" -> SoQLText("12/31/2013"))),
                   UpsertAsSoQL(Map("geom" -> toSoQLPoint(point2), "date" -> SoQLText("11/30/2013"))),
                   DeleteAsCJson(JString("efgh-5678")),
                   UpsertAsSoQL(Map("geom" -> toSoQLPoint(point3), "date" -> SoQLText("12/4/2013"))),
                   UpsertAsSoQL(Map("geom" -> toSoQLPoint(point4), "date" -> SoQLText("1/14/2014"))),
                   DeleteAsCJson(JString("ijkl-9012"))
                 )

  val computeStrategy = ComputationStrategyRecord(ComputationStrategyType.GeoRegion, false,
                                                  Some(Seq("geom")),
                                                  Some(JObject(Map("region" -> JString("wards")))))
  val columnSpec = MinimalColumnRecord(ColumnId("foo"), ColumnName("ward_id"), SoQLText, false,
                                       Some(computeStrategy))
}

class GeospaceHandlerTest extends FunSuite
with MustMatchers with Assertions with BeforeAndAfterAll with GeospaceHandlerData {
  import collection.JavaConverters._
  import ComputationHandler._
  import org.mockserver.model.HttpRequest._
  import org.mockserver.model.HttpResponse._
  import org.mockserver.model.StringBody

  val port = 51234
  var server: ClientAndServer = _

  val testConfig = ConfigFactory.parseMap(Map(
                     "port" -> port,
                     "batch-size" -> 2
                   ).asJava)

  val handler = new GeospaceHandler(testConfig)

  override def beforeAll {
    server = ClientAndServer.startClientAndServer(port)
  }

  override def afterAll {
    server.stop()
  }

  private def mockGeocodeRoute(bodyRegex: String, returnedBody: String) {
    server.when(request.withMethod("POST").
                        withPath("/experimental/regions/wards/geocode").
                        withBody(StringBody.regex(bodyRegex))).
           respond(response.withStatusCode(200).
                            withHeader(new Header("Content-Type", "application/json; charset=utf-8")).
                            withBody(returnedBody))
  }

  test("HTTP geocoder works with mock HTTP server") {
    mockGeocodeRoute(".+122.+", """["Wards.1"]""")
    mockGeocodeRoute(".+121.+", """["Wards.2"]""")
    mockGeocodeRoute(".+120.+", """["","Wards.5"]""")
    val expectedIds = Iterator("Wards.1", "Wards.2", "", "Wards.5")
    val expectedRows = testRows.map {
      case UpsertAsSoQL(map) => UpsertAsSoQL(map + ("ward_id" -> SoQLText(expectedIds.next)))
      case d: DeleteAsCJson  => d
    }
    val newRows = handler.compute(testRows.toIterator, columnSpec)
    newRows.toSeq must equal (expectedRows)
  }

  test("Will throw UnknownColumnEx if source column missing") {
    intercept[UnknownColumnEx] {
      val rows = Seq(Map("date" -> SoQLText("12/31/2013")))
      // NOTE: must call next on an iterator otherwise computation doesn't start
      handler.compute(rows.map(UpsertAsSoQL(_)).toIterator, columnSpec).next
    }
  }

  test("handler.compute() returns lazy iterator") {
    // The way we verify this is a variant of above test.  Unless we call next(), errors in the input
    // will not result in an exception because processing hasn't started yet
    val rows = Seq(Map("date" -> SoQLText("12/31/2013")))    // geom column missing
    handler.compute(rows.map(UpsertAsSoQL(_)).toIterator, columnSpec)
  }

  test("Will throw MaltypedDataEx if source column not right SoQLType") {
    def converter(s: String) = JsonColumnRep.forClientType(SoQLMultiLine).fromJValue(JsonReader.fromString(s))

    // If not MultiLine
    intercept[MaltypedDataEx] {
      val rows = Seq(Map("geom" -> converter(multiLine).get))
      handler.compute(rows.map(UpsertAsSoQL(_)).toIterator, columnSpec).next
    }

  }
}