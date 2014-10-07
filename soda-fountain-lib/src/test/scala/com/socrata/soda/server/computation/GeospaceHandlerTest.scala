package com.socrata.soda.server.computation

import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.{WireMock => WM}
import com.github.tomakehurst.wiremock.stubbing.Scenario
import com.rojoma.json.ast._
import com.rojoma.json.io.JsonReader
import com.socrata.soda.server.highlevel.RowDataTranslator
import com.socrata.soda.server.highlevel.RowDataTranslator.{DeleteAsCJson, UpsertAsSoQL}
import com.socrata.soda.server.id.ColumnId
import com.socrata.soda.server.persistence.{MinimalColumnRecord, ComputationStrategyRecord}
import com.socrata.soda.server.wiremodels.{ComputationStrategyType, JsonColumnRep}
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types._
import com.socrata.thirdparty.curator.{CuratorBroker, CuratorServiceIntegration}
import com.typesafe.config.ConfigFactory
import java.math.{ BigDecimal => BD }
import org.scalatest._

trait GeospaceHandlerData {
  val point1 = "{\"type\":\"Point\",\"coordinates\":[47.6303,-122.3148]}"
  val point2 = "{\"type\":\"Point\",\"coordinates\":[48.6303,-121.3148]}"
  val point3 = "{\"type\":\"Point\",\"coordinates\":[49.6303,-120.3148]}"
  val point4 = "{\"type\":\"Point\",\"coordinates\":[50.11,  -119.98]}"
  val multiLine = """{"type":"MultiLineString","coordinates":[[[100,0.123456789012],[101,1]],[[102,2],[103,3]]]}"""

  val pointRep = JsonColumnRep.forClientType(SoQLPoint)
  def toSoQLPoint(str: String) = pointRep.fromJValue(JsonReader.fromString(str)).get.asInstanceOf[SoQLPoint]

  val testRows = Seq[RowDataTranslator.Computable](
                   DeleteAsCJson(JString("abcd-1234")),
                   UpsertAsSoQL(Map("geom-1234" -> toSoQLPoint(point1), "date-1234" -> SoQLText("12/31/2013"))),
                   UpsertAsSoQL(Map("geom-1234" -> toSoQLPoint(point2), "date-1234" -> SoQLText("11/30/2013"))),
                   DeleteAsCJson(JString("efgh-5678")),
                   UpsertAsSoQL(Map("geom-1234" -> toSoQLPoint(point3), "date-1234" -> SoQLText("12/4/2013"))),
                   UpsertAsSoQL(Map("geom-1234" -> toSoQLPoint(point4), "date-1234" -> SoQLText("1/14/2014"))),
                   DeleteAsCJson(JString("ijkl-9012"))
                 )

  val computeStrategy = ComputationStrategyRecord(ComputationStrategyType.GeoRegion, false,
                                                  Some(Seq("geom-1234")),
                                                  Some(JObject(Map("region" -> JString("wards")))))
  val columnSpec = MinimalColumnRecord(ColumnId("ward-1234"), ColumnName("ward_id"), SoQLText, false,
                                       Some(computeStrategy))
}

class GeospaceHandlerTest extends FunSuite
with MustMatchers with Assertions with BeforeAndAfterAll with BeforeAndAfterEach with GeospaceHandlerData
with CuratorServiceIntegration {
  override val curatorConfigPrefix = "com.socrata.soda-fountain.curator"

  import collection.JavaConverters._
  import ComputationHandler._

  val port = 51234
  val mockServer = new WireMockServer(wireMockConfig.port(port))
  lazy val broker = new CuratorBroker(discovery, "localhost", "geospace", None)
  lazy val cookie = broker.register(port)

  val testConfig = ConfigFactory.parseMap(Map(
                     "service-name" -> "geospace",
                     "batch-size"   -> 2,
                     "max-retries"  -> 1,
                     "retry-wait"   -> "500ms"
                   ).asJava)

  lazy val handler = new GeospaceHandler(testConfig, discovery)

  override def beforeAll {
    startServices()
    mockServer.start()
    WM.configureFor("localhost", port)
    cookie
  }

  override def afterAll {
    broker.deregister(cookie)
    mockServer.stop()
    stopServices()
  }

  override def beforeEach {
    WM.reset()
  }

  private def mockGeocodeRoute(bodyRegex: String, returnedBody: String, returnedCode: Int = 200) {
    WM.stubFor(WM.post(WM.urlEqualTo("/experimental/regions/wards/geocode")).
               withRequestBody(WM.matching(bodyRegex)).
               willReturn(WM.aResponse()
                         .withStatus(returnedCode)
                         .withHeader("Content-Type", "application/vnd.geo+json; charset=utf-8")
                         .withBody(returnedBody)))
  }

  private def mockGeocodeScene(bodyRegex: String, returnedBody: String, returnedCode: Int = 200,
                               sceneStart: String = Scenario.STARTED, sceneEnd: String = "step2") {
    WM.stubFor(WM.post(WM.urlEqualTo("/experimental/regions/wards/geocode")).
               inScenario("default").whenScenarioStateIs(sceneStart).
               withRequestBody(WM.matching(bodyRegex)).
               willReturn(WM.aResponse()
                         .withStatus(returnedCode)
                         .withHeader("Content-Type", "application/vnd.geo+json; charset=utf-8")
                         .withBody(returnedBody)).
               willSetStateTo(sceneEnd))
  }

  test("HTTP geocoder works with mock HTTP server") {
    mockGeocodeRoute(".+122.+", """[1]""")
    mockGeocodeRoute(".+121.+", """[2]""")
    mockGeocodeRoute(".+120.+", """[null,5]""")
    val expectedIds = Iterator(Some(1), Some(2), None, Some(5))
    val expectedRows = testRows.map {
      case UpsertAsSoQL(map) =>
        val nextExpected = expectedIds.next()
        if (nextExpected.isDefined) {
          UpsertAsSoQL(map + ("ward-1234" -> SoQLNumber(new BD(nextExpected.get))))
        } else {
          UpsertAsSoQL(map)
        }
      case d: DeleteAsCJson  => d
    }
    val newRows = handler.compute(testRows.toIterator, columnSpec)
    newRows.toSeq must equal (expectedRows)
  }

  test("Retry works") {
    val testRow = UpsertAsSoQL(
      Map("geom-1234" -> toSoQLPoint(point1), "date-1234" -> SoQLText("12/31/2013")))
    val expectedRow = UpsertAsSoQL(
      Map("geom-1234" -> toSoQLPoint(point1), "date-1234" -> SoQLText("12/31/2013"), "ward-1234" -> SoQLNumber(new BD(1))))

    // Set up the mock server to fail on the first attempt,
    // succeed on the second attempt, then fail on the third attempt.
    // GeospaceHandler is configured to retry once, so the second attempt should succeed.
    mockGeocodeRoute(".+122.+", "", 500)
    mockGeocodeRoute(".+122.+", """[1]""", 200)
    mockGeocodeRoute(".+122.+", "", 500)

    val newRows = handler.compute(Iterator(testRow), columnSpec)
    newRows.toSeq must equal (Stream(expectedRow))
  }

  test("Will return empty featureIds if source column missing for some rows") {
    val rows = Seq(UpsertAsSoQL(Map("date-1234" -> SoQLText("12/31/2013"))),
                   UpsertAsSoQL(Map("date-1234" -> SoQLText("12/31/2014"))),
                   UpsertAsSoQL(Map("date-1234" -> SoQLText("12/31/2015")))) ++ testRows

    mockGeocodeScene(".+122.+", """[3,4]""", sceneEnd = "nowReturn200")
    mockGeocodeScene(".+120.+", """[null]""", sceneStart = "nowReturn200", sceneEnd = "now500")
    mockGeocodeScene(".+119.+", """[6]""",   sceneStart = "now500", sceneEnd = "NEVER")
    val expectedIds = Iterator(None, None, None, Some(3), Some(4), None, Some(6))

    val expectedRows = rows.map {
      case UpsertAsSoQL(map) =>
        val nextExpected = expectedIds.next()
        if (nextExpected.isDefined) {
          UpsertAsSoQL(map + ("ward-1234" -> SoQLNumber(new BD(nextExpected.get))))
        } else {
          UpsertAsSoQL(map)
        }
      case d: DeleteAsCJson  => d
    }

    val newRows = handler.compute(rows.toIterator, columnSpec)
    newRows.toSeq must equal (expectedRows)
  }

  test("Will return empty featureIds if source column missing for all rows") {
    val rows = Seq(UpsertAsSoQL(Map("date" -> SoQLText("12/31/2013"))),
      UpsertAsSoQL(Map("date-1234" -> SoQLText("12/31/2014"))),
      UpsertAsSoQL(Map("date-1234" -> SoQLText("12/31/2015"))))

    val newRows = handler.compute(rows.toIterator, columnSpec)
    newRows.toSeq must equal (rows)
  }

  test("handler.compute() returns lazy iterator") {
    // The way we verify this is a variant of above test.  Unless we call next(), errors in the input
    // will not result in an exception because processing hasn't started yet
    val rows = Seq(Map("date-1234" -> SoQLText("12/31/2013")))    // geom column missing
    handler.compute(rows.map(UpsertAsSoQL(_)).toIterator, columnSpec)
  }

  test("Will throw MaltypedDataEx if source column not right SoQLType") {
    def converter(s: String) = JsonColumnRep.forClientType(SoQLMultiLine).fromJValue(JsonReader.fromString(s))

    // If not MultiLine
    intercept[MaltypedDataEx] {
      val rows = Seq(Map("geom-1234" -> converter(multiLine).get))
      handler.compute(rows.map(UpsertAsSoQL(_)).toIterator, columnSpec).next
    }

  }
}