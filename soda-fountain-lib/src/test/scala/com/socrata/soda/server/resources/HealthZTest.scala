package com.socrata.soda.server.resources

import com.socrata.soda.server.SodaRequest
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.springframework.mock.web.MockHttpServletResponse

class HealthZTest extends AnyFunSuite with Matchers with MockFactory {
  test("All services up") {
    val response = testResource(up = true)
    response.getStatus should be (200)
  }

  private def testResource(up: Boolean): MockHttpServletResponse = {
    val response = new MockHttpServletResponse()
    val request = mock[SodaRequest]
    HealthZ.service.get(request)(response)
    response
  }
}
