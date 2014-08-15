package com.socrata.soda.server.resources

import com.socrata.soda.clients.geospace.GeospaceClient
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.{Matchers, FunSuite}
import org.springframework.mock.web.{MockHttpServletResponse, MockHttpServletRequest}

class HealthZTest extends FunSuite with Matchers with MockFactory {

  test("All services up") {
    testResource(true, 200)
  }

  test("Geospace down") {
    testResource(false, 500)
  }

  private def testResource(geospaceUp: Boolean, expectedResponseCode: Int) {
    val geospace = mock[GeospaceClient]
    geospace.expects('versionCheck)().returning(geospaceUp)

    val healthZ = HealthZ(geospace)
    val response = new MockHttpServletResponse()

    healthZ.service.get(new MockHttpServletRequest())(response)
    response.getStatus should equal (expectedResponseCode)
  }
}
