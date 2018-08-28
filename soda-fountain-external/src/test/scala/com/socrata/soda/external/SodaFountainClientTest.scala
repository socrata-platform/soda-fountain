package com.socrata.soda.external

import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.io.{JsonReader, CompactJsonWriter}
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.{WireMock => WM, MappingBuilder, UrlMatchingStrategy}
import com.github.tomakehurst.wiremock.core.WireMockConfiguration._
import com.socrata.http.client.exceptions.ConnectFailed
import com.socrata.soda.external.SodaFountainClient._
import com.socrata.curator.{CuratorBroker, CuratorServiceIntegration}
import com.socrata.curator.ServerProvider._
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSuite, Matchers, BeforeAndAfterAll}

class SodaFountainClientTest extends FunSuite with Matchers with BeforeAndAfterAll with CuratorServiceIntegration {
  val mockServerPort = 1234
  val mockServer = new WireMockServer(wireMockConfig.port(mockServerPort))

  lazy val broker = new CuratorBroker(discovery, "localhost", "soda-fountain", None)
  lazy val cookie = broker.register(mockServerPort)

  case object MyCustomNoServersException extends Exception
  lazy val sodaFountain = new SodaFountainClient(httpClient,
                                                 discovery,
                                                 "soda-fountain",
                                                 curatorConfig.connectTimeout,
                                                 1,
                                                 RetryOnAllExceptionsDuringInitialRequest,
                                                 throw MyCustomNoServersException)

  override def getFallback = ConfigFactory.load("client-test-config")

  override def beforeAll(): Unit = {
    startServices()            // Start in-process ZK, Curator, service discovery
    mockServer.start()
    cookie                     // register mock HTTP service with Curator/ZK
    sodaFountain.start()       // start soda fountain client
    WM.configureFor("localhost", mockServerPort)
  }

  override def afterAll(): Unit = {
    sodaFountain.close()
    broker.deregister(cookie)
    mockServer.stop()
    stopServices()
  }

  private def mockSodaRoute(httpMethod: UrlMatchingStrategy => MappingBuilder,
                            route: String,
                            status: Int,
                            returnedBody: String,
                            contentType: String = "application/json"): Unit = {
    WM.stubFor(httpMethod(WM.urlEqualTo(route)).
      willReturn(WM.aResponse()
      .withStatus(status)
      .withHeader("Content-Type", s"$contentType; charset=utf-8")
      .withBody(returnedBody)))
  }

  test("Create") {
    val schema = JObject(Map("resource_name" -> JString("foo"), "name" -> JString("Hello World!")))
    mockSodaRoute(WM.post, "/dataset", 201, CompactJsonWriter.toString(schema))
    val Response(code, Some(body)) = sodaFountain.create(schema)
    code should be (201)
    body should be (schema)
  }

  test("Publish") {
    mockSodaRoute(WM.post, "/dataset-copy/foo/_DEFAULT_", 201, "")
    val Response(code, body) = sodaFountain.publish("foo")
    code should be (201)
    body should be (None)
  }

  test("Upsert") {
    val upsertValue = JArray(Seq(JObject(Map("name" -> JString("giraffe")))))
    val upsertResult = JObject(Map("typ" -> JString("insert"),
                                   "id"  -> JString("row-j8qz.au8h.rfp6"),
                                   "ver" -> JString("rv-csvd~ec2x~i33b")))
    mockSodaRoute(WM.post, "/resource/foo", 200, CompactJsonWriter.toString(upsertResult))
    val Response(code, Some(body)) = sodaFountain.upsert("foo", upsertValue)
    code should be (200)
    body should be (upsertResult)
  }

  test("Query JSON as default") {
    mockSodaRoute(WM.get, "/resource/foo", 200, """[{ "foo" : "bar" }]""")
    val Response(code, Some(body)) = sodaFountain.query("foo")
    code should be (200)
    body should be (JArray(Seq(JObject(Map("foo" -> JString("bar"))))))
  }

  test("Query JSON explicitly") {
    mockSodaRoute(WM.get, "/resource/foo.json", 200, """[{ "foo" : "bar" }]""")
    val Response(code, Some(body)) = sodaFountain.query("foo", Some("json"))
    code should be (200)
    body should be (JArray(Seq(JObject(Map("foo" -> JString("bar"))))))
  }

  test("Query GeoJSON") {
    mockSodaRoute(WM.get, "/resource/foo.geojson", 200, """[{ "foo" : "bar" }]""", "application/vnd.geo+json")
    val Response(code, Some(body)) = sodaFountain.query("foo", Some("geojson"))
    code should be (200)
    body should be (JArray(Seq(JObject(Map("foo" -> JString("bar"))))))
  }

  test("Schema") {
    val schema = """{"resource_name":"hello_world",
                   | "name":"Hello World!",
                   | "description":"",
                   | "row_identifier":":id",
                   | "locale":"en_US",
                   | "stage":"Published",
                   | "columns":{"name":{"id":"x8p7-e3wx","field_name":"name","name":"name","description":"","datatype":"text"},
                   |            ":version":{"id":":version","field_name":":version","name":":version","description":"","datatype":"row_version"},
                   |            ":created_at":{"id":":created_at","field_name":":created_at","name":":created_at","description":"","datatype":"fixed_timestamp"},
                   |            "description":{"id":"ydep-8avr","field_name":"description","name":"description","description":"","datatype":"text"},
                   |            ":updated_at":{"id":":updated_at","field_name":":updated_at","name":":updated_at","description":"","datatype":"fixed_timestamp"},
                   |            ":id":{"id":":id","field_name":":id","name":":id","description":"","datatype":"row_identifier"}}}""".stripMargin
    mockSodaRoute(WM.get, "/dataset/foo", 200, schema)

    val Response(code, Some(body)) = sodaFountain.schema("foo")
    code should be (200)
    body should be (JsonReader.fromString(schema))
  }

  test("Handle error (>400) HTTP response") {
    mockSodaRoute(WM.get,
                  "/resource/foo",
                  400,
                  """{ "message" : "soda.dataset.not-found",
                   |   "errorCode" : "soda.dataset.not-found",
                   |   "data" : { "dataset" : "foo" } }""".stripMargin)
    val Response(code, Some(body)) = sodaFountain.query("foo")
    code should be (400)
    body should be (JObject(Map("message" -> JString("soda.dataset.not-found"),
                                           "errorCode" -> JString("soda.dataset.not-found"),
                                           "data" -> JObject(Map("dataset" -> JString("foo"))))))
  }

  test("Soda Fountain unavailable") {
    val sfNotInZk = new SodaFountainClient(httpClient,
      discovery,
      "soda-fountain-nonexistent",
      curatorConfig.connectTimeout,
      1,
      RetryOnAllExceptionsDuringInitialRequest,
      throw MyCustomNoServersException)

    val result = sfNotInZk.query("foo")
    result.getClass should be (classOf[Failed])
    result.asInstanceOf[Failed].exception.getClass should be (MyCustomNoServersException.getClass)
  }

  test("Zookeeper unavailable") {
    // Stop the mock server so the ZK endpoint is unavailable
    mockServer.stop()

    val result = sodaFountain.query("foo")
    result.getClass should be (classOf[Failed])
    result.asInstanceOf[Failed].exception.getClass should be (MyCustomNoServersException.getClass)

    // Restart the mock server so the rest of the tests run normally
    mockServer.start()
  }
}
