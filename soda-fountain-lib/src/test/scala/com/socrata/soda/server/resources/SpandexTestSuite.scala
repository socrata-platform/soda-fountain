package com.socrata.soda.server.resources

import java.net.URI
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.{MappingBuilder, UrlMatchingStrategy, WireMock}
import com.github.tomakehurst.wiremock.core.WireMockConfiguration._
import com.github.tomakehurst.wiremock.http.ContentTypeHeader
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

trait SpandexTestSuite extends AnyFunSuiteLike with BeforeAndAfterAll with BeforeAndAfterEach {
  val mockServerHost = "localhost"
  val mockServerPort = 51200 + (util.Random.nextInt % 100)
  val mockServer = new WireMockServer(wireMockConfig.port(mockServerPort))

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    mockServer.start()
    WireMock.configureFor(mockServerHost, mockServerPort)
  }

  override protected def afterAll(): Unit = {
    mockServer.stop()
    super.afterAll()
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    WireMock.reset()
  }
  
  def mockServerUri(path: String): URI = {
    val separator = if (path.startsWith("/")) "" else "/"
    new URI(s"http://$mockServerHost:$mockServerPort$separator$path")
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
