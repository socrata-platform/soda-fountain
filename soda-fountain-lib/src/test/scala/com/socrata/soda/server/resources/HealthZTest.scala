package com.socrata.soda.server.resources

import com.socrata.http.server.HttpRequest
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.{Matchers, FunSuite}
import org.springframework.mock.web.MockHttpServletResponse

class HealthZTest extends FunSuite with Matchers with MockFactory {
  test("All services up") {
    val response = testResource(up = true)
    response.getStatus should be (200)
  }

  private def testResource(up: Boolean): MockHttpServletResponse = {
    val response = new MockHttpServletResponse()
    val request = mock[HttpRequest]
    HealthZ.service.get(request)(response)
    response
  }
}
