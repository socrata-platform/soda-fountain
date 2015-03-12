package com.socrata.soda.server.computation

import java.math.{BigDecimal => BD}

import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.codec._
import com.rojoma.json.v3.conversions._
import com.rojoma.json.v3.io.JsonReader
import com.socrata.soda.server.highlevel.RowDataTranslator
import com.socrata.soda.server.highlevel.RowDataTranslator.{DeleteAsCJson, UpsertAsSoQL}
import com.socrata.soda.server.id.ColumnId
import com.socrata.soda.server.persistence.{ColumnRecordLike, ComputationStrategyRecord, MinimalColumnRecord}
import com.socrata.soda.server.wiremodels.{ComputationStrategyType, JsonColumnRep}
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types._
import com.socrata.thirdparty.curator.{CuratorBroker, CuratorServiceIntegration}
import com.typesafe.config.ConfigFactory
import org.apache.curator.x.discovery._
import org.mockito.Matchers.{any, anyString}
import org.mockito.Mockito.{verify, when}
import org.mockserver.integration.ClientAndServer
import org.mockserver.model.Header
import org.scalatest._
import org.scalatest.mock.MockitoSugar

trait GeoregionMatchOnPointHandlerData {
  val point1 = """{"type":"Point","coordinates":[47.6303,-122.3148]}"""
  val point2 = """{"type":"Point","coordinates":[48.6303,-121.3148]}"""
  val point3 = """{"type":"Point","coordinates":[49.6303,-120.3148]}"""
  val point4 = """{"type":"Point","coordinates":[50.11,  -119.98]}"""
  val multiLine = """{"type":"MultiLineString","coordinates":[[[100,0.123456789012],[101,1]],[[102,2],[103,3]]]}"""

  val pointRep = JsonColumnRep.forClientType(SoQLPoint)
  def toSoQLPoint(str: String) = pointRep.fromJValue(JsonReader.fromString(str).toV2).get.asInstanceOf[SoQLPoint]
  def sourceColumn(id: String) = MinimalColumnRecord(ColumnId(id), ColumnName("test"), SoQLNull, false, None)

  val testRows = Seq[RowDataTranslator.Computable](
    DeleteAsCJson(JString("abcd-1234")),
    UpsertAsSoQL(Map("geom-1234" -> toSoQLPoint(point1), "date-1234" -> SoQLText("12/31/2013"))),
    UpsertAsSoQL(Map("geom-1234" -> toSoQLPoint(point2), "date-1234" -> SoQLText("11/30/2013"))),
    DeleteAsCJson(JString("efgh-5678")),
    UpsertAsSoQL(Map("geom-1234" -> toSoQLPoint(point3), "date-1234" -> SoQLText("12/4/2013"))),
    UpsertAsSoQL(Map("geom-1234" -> toSoQLPoint(point4), "date-1234" -> SoQLText("1/14/2014"))),
    DeleteAsCJson(JString("ijkl-9012"))
  )

  val testRow = UpsertAsSoQL(
    Map("geom-1234" -> toSoQLPoint(point1), "date-1234" -> SoQLText("12/31/2013")))

  val computeStrategy = ComputationStrategyRecord(ComputationStrategyType.GeoRegionMatchOnPoint, false,
    Some(Seq(sourceColumn("geom-1234"))),
    Some(JObject(Map("region" -> JString("wards")))))
  val columnSpec = MinimalColumnRecord(ColumnId("ward-1234"), ColumnName("ward_id"), SoQLText, false,
    Some(computeStrategy))
}

class GeoregionMatchOnPointHandlerTest extends FunSuite
    with MustMatchers with BeforeAndAfterAll with BeforeAndAfterEach with GeoregionMatchOnPointHandlerData
    with CuratorServiceIntegration with MockitoSugar {
  override def configPrefix = "com.socrata.soda-fountain"

  import com.socrata.soda.server.computation.ComputationHandler._
  import org.mockserver.model.HttpRequest._
  import org.mockserver.model.HttpResponse._
  import org.mockserver.model.StringBody
  import scala.collection.JavaConverters._

  val port = 51234
  var server: ClientAndServer = _
  lazy val broker = new CuratorBroker(discovery, "localhost", "geospace", None)
  lazy val cookie = broker.register(port)

  val testConfig = ConfigFactory.parseMap(Map(
    "service-name"    -> "geospace",
    "batch-size"      -> 2,
    "max-retries"     -> 1,
    "retry-wait"      -> "500ms",
    "connect-timeout" -> "5s",
    "read-timeout"    -> "5s"
  ).asJava)

  lazy val handler = new GeoregionMatchOnPointHandler(testConfig, discovery)

  override def beforeAll() {
    startServices()
    server = ClientAndServer.startClientAndServer(port)
    cookie
  }

  override def afterAll() {
    broker.deregister(cookie)
    server.stop
    stopServices()
  }

  override def beforeEach() {
    server.reset
  }

  private def mockGeocodeRoute(bodyRegex: String, returnedBody: String, returnedCode: Int = 200) {
    server.when(request.withMethod("POST").
      withPath("/v1/regions/wards/geocode").
      withBody(StringBody.regex(bodyRegex))).
      respond(response.withStatusCode(returnedCode).
        withHeader(new Header("Content-Type", "application/json; charset=utf-8")).
        withBody(returnedBody))
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
    val expectedRow = UpsertAsSoQL(
      Map("geom-1234" -> toSoQLPoint(point1), "date-1234" -> SoQLText("12/31/2013"), "ward-1234" -> SoQLNumber(new BD(1))))

    // Set up the mock server to fail on the first attempt,
    // succeed on the second attempt, then fail on the third attempt.
    // GeoregionMatchHandler is configured to retry once, so the second attempt should succeed.
    mockGeocodeRoute(".+122.+", "", 500)
    mockGeocodeRoute(".+122.+", """[1]""", 200)
    mockGeocodeRoute(".+122.+", "", 500)

    val newRows = handler.compute(Iterator(testRow), columnSpec)
    newRows.toSeq must equal (Stream(expectedRow))
  }

  test("Will return empty featureIds if source column missing or null for some rows") {
    val rows = Seq(UpsertAsSoQL(Map("date-1234" -> SoQLText("12/31/2013"))),
      UpsertAsSoQL(Map("geom-1234" -> SoQLNull, "date-1234" -> SoQLText("12/31/2014"))),
      UpsertAsSoQL(Map("date-1234" -> SoQLText("12/31/2015")))) ++ testRows

    mockGeocodeRoute(".+122.+", """[3,4]""")
    mockGeocodeRoute(".+120.+", """[null]""")
    mockGeocodeRoute(".+119.+", """[6]""")
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

  test("optionIntCodec.encode wraps JsonCodec[Int].encode") {
    handler.optionIntCodec.encode(Some(1234)) must equal (JsonEncode[Int].encode(1234))
    handler.optionIntCodec.encode(None) must equal (JNull)
  }

  test("handler.compute() returns lazy iterator") {
    // The way we verify this is a variant of above test.  Unless we call next(), errors in the input
    // will not result in an exception because processing hasn't started yet
    val rows = Seq(Map("date-1234" -> SoQLText("12/31/2013")))    // geom column missing
    handler.compute(rows.map(UpsertAsSoQL(_)).toIterator, columnSpec)
  }

  test("handler.close() closes provider") {
    val mockDiscovery = mock[ServiceDiscovery[Any]]
    val mockBuilder = mock[ServiceProviderBuilder[Any]]
    val mockProvider = mock[ServiceProvider[Any]]

    when(mockDiscovery.serviceProviderBuilder()).thenReturn(mockBuilder)
    when(mockBuilder.providerStrategy(any[ProviderStrategy[Any]])).thenReturn(mockBuilder)
    when(mockBuilder.serviceName(anyString)).thenReturn(mockBuilder)
    when(mockBuilder.build()).thenReturn(mockProvider)

    val handler = new GeoregionMatchOnPointHandler(testConfig, mockDiscovery)
    handler.close()

    verify(mockProvider).close()
  }

  test("Will throw MaltypedDataEx if source column not right SoQLType") {
    def converter(s: String) = JsonColumnRep.forClientType(SoQLMultiLine).fromJValue(JsonReader.fromString(s).toV2)

    // If not MultiLine
    intercept[MaltypedDataEx] {
      val rows = Seq(Map("geom-1234" -> converter(multiLine).get))
      handler.compute(rows.map(UpsertAsSoQL).toIterator, columnSpec).next
    }
  }

  test("Will throw RuntimeException if unable to get a Geospace instance") {
    val mockDiscovery = mock[ServiceDiscovery[Any]]
    val mockBuilder = mock[ServiceProviderBuilder[Any]]
    val mockProvider = mock[ServiceProvider[Any]]

    when(mockDiscovery.serviceProviderBuilder()).thenReturn(mockBuilder)
    when(mockBuilder.providerStrategy(any[ProviderStrategy[Any]])).thenReturn(mockBuilder)
    when(mockBuilder.serviceName(anyString)).thenReturn(mockBuilder)
    when(mockBuilder.build()).thenReturn(mockProvider)

    val handler = new GeoregionMatchOnPointHandler(testConfig, mockDiscovery)
    the [RuntimeException] thrownBy {
      handler.urlPrefix
    } must have message "Unable to get Geospace instance from Curator/ZK"
  }

  test("Will throw ComputationEx if computing an unsupported row type") {
    val ex = the [ComputationEx] thrownBy {
      handler.compute(Iterator(null), columnSpec).
        foreach(Function.const(())) // Force evaluation of the iterator.
    }

    ex.message must equal ("Unsupported row update type passed into GeoregionMatchOnPointHandler")
  }

  test("Will throw IllegalArgumentException when column is not computed") {
    val badColumnSpec = mock[ColumnRecordLike]
    when(badColumnSpec.computationStrategy).thenReturn(None)

    val ex = the [IllegalArgumentException] thrownBy {
      handler.compute(testRows.iterator, badColumnSpec)
    }

    ex.getMessage must include ("No computation strategy found")
  }

  test("Will throw IllegalArgumentException when missing region parameter") {
    val badColumnSpec = mock[ColumnRecordLike]
    when(badColumnSpec.computationStrategy).thenReturn(Some(
      ComputationStrategyRecord(
        ComputationStrategyType.GeoRegionMatchOnPoint,
        false,
        Some(Seq(sourceColumn(""))),
        Some(JObject(Map())))))

    val ex = the [IllegalArgumentException] thrownBy {
      handler.compute(testRows.iterator, badColumnSpec).next()
    }
    ex.getMessage must include ("parameters does not contain 'region'")
  }

  test("Will throw IllegalArgumentException when missing source columns") {
    val badColumnSpec = mock[ColumnRecordLike]
    when(badColumnSpec.computationStrategy).thenReturn(Some(
      ComputationStrategyRecord(
        ComputationStrategyType.GeoRegionMatchOnPoint,
        false,
        Some(Seq()),
        null)))

    val ex = the [IllegalArgumentException] thrownBy {
      handler.compute(testRows.iterator, badColumnSpec)
    }
    ex.getMessage must include ("Source column was not defined " +
      "in computation strategy")
  }

  test("Will throw ComputationEx when post does not return 200") {
    mockGeocodeRoute(".+", "[]", 300)

    val ex = the [ComputationEx] thrownBy {
      handler.compute(Iterator(testRow), columnSpec).
        foreach(Function.const(())) // Force evaluation of the iterator.
    }

    val message = ex.getMessage

    ex.underlying must be (None)
    message must include ("Error: HTTP")
    message must include ("got response code 300, body []")
  }

  test("Will throw ComputationEx when post returns invalid data") {
    mockGeocodeRoute(".+", "null", 200)

    val ex = the [ComputationEx] thrownBy {
      handler.compute(Iterator(testRow), columnSpec).
        foreach(Function.const(())) // Force evaluation of the iterator.
    }

    val message = ex.getMessage

    ex.underlying must be (None)
    message must include ("Error parsing JSON response")
  }

  test("Will throw when retries fail") {
    mockGeocodeRoute(".+", "", 500)
    mockGeocodeRoute(".+", "", 500) // Out of retries here.
    mockGeocodeRoute(".+", "", 200)

    val ex = the [ComputationEx] thrownBy {
      handler.compute(Iterator(testRow), columnSpec).
        foreach(Function.const(())) // Force evaluation of the iterator.
    }

    val message = ex.getMessage

    ex.underlying.isDefined must be (true)
    message must include ("HTTP Error reading")
  }
}
