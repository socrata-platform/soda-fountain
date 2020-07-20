package com.socrata.soda.server.resources

import java.net.URI
import java.util.concurrent.Executors
import javax.servlet.http.{HttpServletRequest, HttpServletResponse => HttpStatus}

import com.rojoma.json.v3.ast.{JNull, JObject, JString}
import com.rojoma.json.v3.util.JsonUtil
import com.rojoma.simplearm.v2._
import com.socrata.http.client._
import com.socrata.http.client.exceptions.{ConnectFailed, ConnectTimeout, ReceiveTimeout}
import com.socrata.http.server.{HttpRequest, ConcreteHttpRequest}
import com.socrata.soda.server.config.{SodaFountainConfig, SuggestConfig}
import com.socrata.soda.server.copy.Published
import com.socrata.soda.server.highlevel.{ColumnDAO, DatasetDAO}
import com.socrata.soda.server.id.{ColumnId, DatasetId, ResourceName}
import com.socrata.soda.server.persistence.{ColumnRecord, DatasetRecord}
import com.socrata.soda.server.util.CloseableExecutorService
import com.socrata.soda.server.SodaRequest
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.SoQLNull
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.joda.time.DateTime
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.Matchers
import org.scalatest.concurrent.Timeouts
import org.scalatest.time.SpanSugar._
import org.springframework.mock.web.MockHttpServletResponse

import scala.language.postfixOps

class SuggestTest extends SpandexTestSuite with Matchers with MockFactory with Timeouts {
  val resourceName = new ResourceName("abcd-1234")
  val expectedDatasetId = "alpha.1234"
  val lifecycleStage = Published
  val columnName = new ColumnName("some_column_name")
  val expectedColumnId = "abcd-1235"
  val suggestText = "nar"
  // TODO: use spandex results case class, must be imported
  val expectedBody = JsonUtil.renderJson(
    """{
        "options": [
          {"text": "NARCOTICS", "score": 42.0}
        ]
      }""")
  val expectedStatusCode = 200

  val datasetRecord = new DatasetRecord(
    resourceName, new DatasetId(expectedDatasetId),
    "", "", "", "", new ColumnId(""), Seq.empty, 0L, None, DateTime.now()
  )
  val columnRecord = new ColumnRecord(
    new ColumnId(expectedColumnId), columnName,
    SoQLNull, false, None
  )

  def withHttpRequest[T](req: HttpServletRequest)(f: HttpRequest => T): T = {
    using(new ResourceScope) { scope =>
      f(new ConcreteHttpRequest(new HttpRequest.AugmentedHttpServletRequest(req), scope))
    }
  }

  def httpClient: HttpClient =
    new HttpClientHttpClient(
      new CloseableExecutorService(Executors.newCachedThreadPool()),
      HttpClientHttpClient.defaultOptions.
        withUserAgent("TEST"))

  val mockConfig = new SodaFountainConfig(
    ConfigFactory.load()
      .withValue("com.socrata.soda-fountain.suggest.host", ConfigValueFactory.fromAnyRef(mockServerHost))
      .withValue("com.socrata.soda-fountain.suggest.port", ConfigValueFactory.fromAnyRef(mockServerPort))
  ).suggest

  def mockSuggest(datasetDao: DatasetDAO = mock[DatasetDAO],
                  columnDao: ColumnDAO = mock[ColumnDAO],
                  httpClient: HttpClient = httpClient,
                  config: SuggestConfig = mockConfig
                   ): Suggest =
    Suggest(datasetDao, columnDao, httpClient, config)

  test("config values defined") {
    Some(mockConfig.host) should be('defined)
    Some(mockConfig.port) should be('defined)
    Some(mockConfig.connectTimeout) should be('defined)
    Some(mockConfig.receiveTimeout) should be('defined)
  }
  test("config value port out of range exception") {
    val configLow = new SodaFountainConfig(
      ConfigFactory.load()
        .withValue("com.socrata.soda-fountain.suggest.port", ConfigValueFactory.fromAnyRef(-1))
    ).suggest
    a[IllegalArgumentException] should be thrownBy {
      mockSuggest(config = configLow)
    }

    val configHigh = new SodaFountainConfig(
      ConfigFactory.load()
        .withValue("com.socrata.soda-fountain.suggest.port", ConfigValueFactory.fromAnyRef(65536))
    ).suggest
    a[IllegalArgumentException] should be thrownBy {
      mockSuggest(config = configHigh)
    }
  }
  test("config value connectTimeout out of range exception") {
    val configLow = new SodaFountainConfig(
      ConfigFactory.load()
        .withValue("com.socrata.soda-fountain.suggest.connect-timeout", ConfigValueFactory.fromAnyRef("-1 s"))
    ).suggest
    a[IllegalArgumentException] should be thrownBy {
      mockSuggest(config = configLow)
    }

    val configHigh = new SodaFountainConfig(
      ConfigFactory.load()
        .withValue("com.socrata.soda-fountain.suggest.connect-timeout", ConfigValueFactory.fromAnyRef("25 d"))
    ).suggest
    a[IllegalArgumentException] should be thrownBy {
      mockSuggest(config = configHigh)
    }
  }
  test("config value receiveTimeout out of range exception") {
    val configLow = new SodaFountainConfig(
      ConfigFactory.load()
        .withValue("com.socrata.soda-fountain.suggest.receive-timeout", ConfigValueFactory.fromAnyRef("-1 s"))
    ).suggest
    a[IllegalArgumentException] should be thrownBy {
      mockSuggest(config = configLow)
    }

    val configHigh = new SodaFountainConfig(
      ConfigFactory.load()
        .withValue("com.socrata.soda-fountain.suggest.receive-timeout", ConfigValueFactory.fromAnyRef("25 d"))
    ).suggest
    a[IllegalArgumentException] should be thrownBy {
      mockSuggest(config = configHigh)
    }
  }

  test("translate dataset name to id - found") {
    val d = mock[DatasetDAO]
    d.expects('getDataset)(resourceName, None).returning(DatasetDAO.Found(datasetRecord))

    mockSuggest(datasetDao = d).datasetId(resourceName) should be(Some(expectedDatasetId))
  }
  test("translate dataset name to id - not found") {
    val d = mock[DatasetDAO]
    d.expects('getDataset)(resourceName, None).returning(DatasetDAO.DatasetNotFound(resourceName))

    mockSuggest(datasetDao = d).datasetId(resourceName) should be(None)
  }
  test("translate dataset name to id - unknown result") {
    val d = mock[DatasetDAO]
    d.expects('getDataset)(resourceName, None).returning(DatasetDAO.Created(datasetRecord))

    a[Exception] should be thrownBy {
      mockSuggest(datasetDao = d).datasetId(resourceName) should be(None)
    }
  }

  test("translate column name to id - found") {
    val c = mock[ColumnDAO]
    c.expects('getColumn)(resourceName, columnName).returning(ColumnDAO.Found(datasetRecord, columnRecord, None))

    mockSuggest(columnDao = c).datacoordinatorColumnId(resourceName, columnName) should be(Some(expectedColumnId))
  }
  test("translate column name to id - not found") {
    val c = mock[ColumnDAO]
    c.expects('getColumn)(resourceName, columnName).returning(ColumnDAO.ColumnNotFound(columnName))

    mockSuggest(columnDao = c).datacoordinatorColumnId(resourceName, columnName) should be(None)
  }
  test("translate column name to id - unknown result") {
    val c = mock[ColumnDAO]
    c.expects('getColumn)(resourceName, columnName).returning(ColumnDAO.Created(columnRecord, None))

    a[Exception] should be thrownBy {
      mockSuggest(columnDao = c).datacoordinatorColumnId(resourceName, columnName) should be(None)
    }
  }

  test("make internal context - all found") {
    val d = mock[DatasetDAO]
    d.expects('getDataset)(resourceName, None).returning(DatasetDAO.Found(datasetRecord))

    val c = mock[ColumnDAO]
    c.expects('getColumn)(resourceName, columnName).returning(ColumnDAO.Found(datasetRecord, columnRecord, None))

    val suggest = mockSuggest(datasetDao = d, columnDao = c)

    val (ds, cn, col) = suggest.internalContext(resourceName, columnName).get
    ds should be(expectedDatasetId)
    cn should be(Published)
    col should be(expectedColumnId)
  }
  test("make internal context - column not found") {
    val d = mock[DatasetDAO]
    d.expects('getDataset)(resourceName, None).returning(DatasetDAO.Found(datasetRecord))

    val c = mock[ColumnDAO]
    c.expects('getColumn)(resourceName, columnName).returning(ColumnDAO.ColumnNotFound(columnName))

    val suggest = mockSuggest(datasetDao = d, columnDao = c)

    val ctx = suggest.internalContext(resourceName, columnName)
    ctx shouldNot be('defined)
  }
  test("make internal context - dataset not found") {
    val d = mock[DatasetDAO]
    d.expects('getDataset)(resourceName, None).returning(DatasetDAO.DatasetNotFound(resourceName))

    val suggest = mockSuggest(datasetDao = d)

    val ctx = suggest.internalContext(resourceName, columnName)
    ctx shouldNot be('defined)
  }

  test("execute external request to spandex") {
    val path = "/"
    val expectedBody = JObject(Map("donut" -> JString("coconut")))
    setSpandexResponse(url = path, body = JsonUtil.renderJson(expectedBody))

    val (code, body) = mockSuggest().getSpandexResponse(mockServerUri(path))
    code should be(HttpStatus.SC_OK)
    body should be(expectedBody)
  }

  test("spandex response non json") {
    val path = "/"
    setSpandexResponse(url = path, body = "this is not json", contentType = "text/plain")
    val (code, body) = mockSuggest().getSpandexResponse(mockServerUri(path))
    code should be(HttpStatus.SC_OK)
    body should be(JNull)
  }

  test("service suggestions - found") {
    val d = mock[DatasetDAO]
    d.expects('getDataset)(resourceName, None).returning(DatasetDAO.Found(datasetRecord))

    val c = mock[ColumnDAO]
    c.expects('getColumn)(resourceName, columnName).returning(ColumnDAO.Found(datasetRecord, columnRecord, None))

    val path = s"/suggest/$expectedDatasetId/$lifecycleStage/$expectedColumnId/$suggestText"
    setSpandexResponse(url = path, body = expectedBody)

    val suggest = mockSuggest(datasetDao = d, columnDao = c)

    val servReq = mockHttpServletRequest()
    withHttpRequest(servReq) { httpReq =>
      val response = new MockHttpServletResponse()
      suggest.service(resourceName, columnName, suggestText).
        get(new SodaRequest {
              val httpRequest = httpReq
            })(response)

      response.getStatus should be(expectedStatusCode)
      response.getContentAsString should be(expectedBody)
    }
  }
  test("service suggestions - dataset not found") {
    val d = mock[DatasetDAO]
    d.expects('getDataset)(resourceName, None).returning(DatasetDAO.DatasetNotFound(resourceName))

    val suggest = mockSuggest(datasetDao = d)

    val httpReq = mock[SodaRequest]
    val response = new MockHttpServletResponse()
    suggest.service(resourceName, columnName, suggestText).get(httpReq)(response)

    response.getStatus should be(HttpStatus.SC_NOT_FOUND)
  }

  test("service samples - found") {
    val d = mock[DatasetDAO]
    d.expects('getDataset)(resourceName, None).returning(DatasetDAO.Found(datasetRecord))

    val c = mock[ColumnDAO]
    c.expects('getColumn)(resourceName, columnName).returning(ColumnDAO.Found(datasetRecord, columnRecord, None))

    val path = s"/suggest/$expectedDatasetId/$lifecycleStage/$expectedColumnId"
    setSpandexResponse(url = path, body = expectedBody)

    val suggest = mockSuggest(datasetDao = d, columnDao = c)

    val servReq = mockHttpServletRequest()
    withHttpRequest(servReq) { httpReq =>
      val response = new MockHttpServletResponse()
      suggest.sampleService(resourceName, columnName).
        get(new SodaRequest {
              val httpRequest = httpReq
            })(response)

      response.getStatus should be(expectedStatusCode)
      response.getContentAsString should be(expectedBody)
    }
  }
  test("service samples - dataset not found") {
    val d = mock[DatasetDAO]
    d.expects('getDataset)(resourceName, None).returning(DatasetDAO.DatasetNotFound(resourceName))

    val suggest = mockSuggest(datasetDao = d)

    val httpReq = mock[SodaRequest]
    val response = new MockHttpServletResponse()
    suggest.sampleService(resourceName, columnName).get(httpReq)(response)

    response.getStatus should be(HttpStatus.SC_NOT_FOUND)
  }

  test("spandex connect failed") {
    failAfter(2 seconds) {
      a[ConnectFailed] should be thrownBy {
        // non existent host
        mockSuggest().getSpandexResponse(new URI("http://255.255.255.255/"))
      }
    }
  }

  test("spandex receive timeout") {
    val path = "/"
    setSpandexResponse(url = path, body = "receive timeout", syntheticDelayMs = 10000)
    failAfter(6 seconds) {
      a[ReceiveTimeout] should be thrownBy {
        mockSuggest().getSpandexResponse(mockServerUri(path))
      }
    }
  }

  test("spandex connect failed - check response status") {
    val d = mock[DatasetDAO]
    d.expects('getDataset)(resourceName, None).returning(DatasetDAO.Found(datasetRecord))

    val c = mock[ColumnDAO]
    c.expects('getColumn)(resourceName, columnName).returning(ColumnDAO.Found(datasetRecord, columnRecord, None))

    val path = s"/suggest/$expectedDatasetId/$lifecycleStage/$expectedColumnId/$suggestText"
    setSpandexResponse(url = path, body = expectedBody)

    val suggest = mockSuggest(datasetDao = d, columnDao = c,
      config = new SuggestConfig(ConfigFactory.parseString(
      """  suggest {
        |    host = "255.255.255.255"
        |    port = 8042
        |    connect-timeout = 1s
        |    receive-timeout = 5s
        |  }
      """.stripMargin), "suggest"))

    val servReq = mockHttpServletRequest()
    withHttpRequest(servReq) { httpReq =>
      val response = new MockHttpServletResponse()
      suggest.service(resourceName, columnName, suggestText).
        get(new SodaRequest {
              val httpRequest = httpReq
            })(response)

      response.getContentType should include("application/json")
      response.getStatus should be(HttpStatus.SC_INTERNAL_SERVER_ERROR)
    }
  }
  test("spandex connect timeout - check response status") {
    val d = mock[DatasetDAO]
    d.expects('getDataset)(resourceName, None).returning(DatasetDAO.Found(datasetRecord))

    val c = mock[ColumnDAO]
    c.expects('getColumn)(resourceName, columnName).returning(ColumnDAO.Found(datasetRecord, columnRecord, None))

    val path = s"/suggest/$expectedDatasetId/$lifecycleStage/$expectedColumnId/$suggestText"
    setSpandexResponse(url = path, body = expectedBody)

    val suggest = mockSuggest(datasetDao = d, columnDao = c,
      config = new SuggestConfig(ConfigFactory.parseString(
        """  suggest {
          |    host = "10.255.255.1"
          |    port = 8042
          |    connect-timeout = 1s
          |    receive-timeout = 5s
          |  }
        """.stripMargin), "suggest"))

    val servReq = mockHttpServletRequest()
    withHttpRequest(servReq) { httpReq =>
      val response = new MockHttpServletResponse()
      suggest.service(resourceName, columnName, suggestText).
        get(new SodaRequest {
              val httpRequest = httpReq
            })(response)

      response.getContentType should include("application/json")
      response.getStatus should be(HttpStatus.SC_INTERNAL_SERVER_ERROR)
    }
  }
  test("spandex receive timeout - check response status") {
    val d = mock[DatasetDAO]
    d.expects('getDataset)(resourceName, None).returning(DatasetDAO.Found(datasetRecord))

    val c = mock[ColumnDAO]
    c.expects('getColumn)(resourceName, columnName).returning(ColumnDAO.Found(datasetRecord, columnRecord, None))

    val path = s"/suggest/$expectedDatasetId/$lifecycleStage/$expectedColumnId/$suggestText"
    setSpandexResponse(url = path, body = expectedBody, syntheticDelayMs = 86400000)

    val suggest = mockSuggest(datasetDao = d, columnDao = c)

    val servReq = mockHttpServletRequest()
    withHttpRequest(servReq) { httpReq =>
      val response = new MockHttpServletResponse()
      suggest.service(resourceName, columnName, suggestText).
        get(new SodaRequest {
              val httpRequest = httpReq
            })(response)

      response.getContentType should include("application/json")
      response.getStatus should be(HttpStatus.SC_INTERNAL_SERVER_ERROR)
    }
  }

  private def mockHttpServletRequest() = {
    val servReq = mock[HttpServletRequest]
    servReq.expects('getQueryString)()
    servReq.expects('getHeader)("X-Socrata-RequestId").returning(null)
    servReq
  }
}
