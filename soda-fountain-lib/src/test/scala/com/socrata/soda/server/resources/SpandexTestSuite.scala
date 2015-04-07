package com.socrata.soda.server.resources

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.{MappingBuilder, UrlMatchingStrategy, WireMock}
import com.github.tomakehurst.wiremock.core.WireMockConfiguration._
import com.github.tomakehurst.wiremock.http.ContentTypeHeader
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike}

trait SpandexTestSuite extends FunSuiteLike with BeforeAndAfterAll with BeforeAndAfterEach {
  val mockServerPort = 51200 + (util.Random.nextInt % 100)
  val mockServer = new WireMockServer(wireMockConfig.port(mockServerPort))

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    mockServer.start()
    WireMock.configureFor("localhost", mockServerPort)
  }

  override protected def afterAll(): Unit = {
    mockServer.stop()
    super.afterAll()
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    WireMock.reset()
  }

  def setSpandexResponse(method: UrlMatchingStrategy => MappingBuilder = WireMock.get,
                         url: String,
                         status: Int = 200,
                         contentType: String = "application/json; charset=utf-8",
                         body: String,
                         syntheticDelayMs: Int = 0): Unit = {
    WireMock.stubFor(method(WireMock.urlMatching(url))
      .willReturn(
        WireMock.aResponse()
          .withStatus(status)
          .withHeader(ContentTypeHeader.KEY, contentType)
          .withBody(body)
          .withFixedDelay(syntheticDelayMs)
      )
    )
  }
}
