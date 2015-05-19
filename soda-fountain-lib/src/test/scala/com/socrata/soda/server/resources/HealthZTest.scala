package com.socrata.soda.server.resources

import com.rojoma.json.v3.ast.{JBoolean, JObject}
import com.rojoma.json.v3.codec.JsonEncode
import com.rojoma.json.v3.io.JsonReader
import com.socrata.http.server.HttpRequest
import com.socrata.soda.clients.regioncoder.RegionCoderClient
import com.socrata.soda.clients.regioncoder.RegionCoderClient._
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.{Matchers, FunSuite}
import org.springframework.mock.web.MockHttpServletResponse

class HealthZTest extends FunSuite with Matchers with MockFactory {
  test("All services up") {
    val response = testResource(up = true)
    response.getStatus should be (200)
  }

  test("region-coder down") {
    val response = testResource(up = false)
    val body = JsonReader.fromString(response.getContentAsString)
    response.getStatus should be (500)
    body should be (JObject(Map(
      "region_coder_ok" -> JBoolean.canonicalFalse,
      "details" -> JsonEncode.toJValue(downResponse))))
  }

  private def upResponse = Success
  private def downResponse =
    Failure("http://localhost:2020/version", 400, """{ "error" : "Danger danger" }""")

  private def testResource(up: Boolean): MockHttpServletResponse = {
    val regionCoder = mock[RegionCoderClient]
    val mockResponse = if (up) upResponse else downResponse
    regionCoder.expects('versionCheck)().returning(mockResponse)

    val healthZ = HealthZ(regionCoder)
    val response = new MockHttpServletResponse()
    val request = mock[HttpRequest]
    healthZ.service.get(request)(response)
    response
  }
}
