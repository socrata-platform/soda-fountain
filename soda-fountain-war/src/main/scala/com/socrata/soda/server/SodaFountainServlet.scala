package com.socrata.soda.server

import com.rojoma.simplearm.v2.ResourceScope
import com.socrata.http.server.HttpRequest
import com.socrata.http.server.HttpRequest.AugmentedHttpServletRequest
import com.socrata.soda.server.config.SodaFountainConfig
import com.typesafe.config.ConfigFactory
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

class SodaFountainServlet extends HttpServlet {
  var fountain: SodaFountain = null

  override def init() {
    val config = new SodaFountainConfig(ConfigFactory.load())
    fountain = new SodaFountain(config)
  }

  override def destroy() {
    fountain.close()
    fountain = null
  }

  // just cut out all the doGet etc nonsense and handle the request ourselves.
  override def service(req: HttpServletRequest, resp: HttpServletResponse) {

    val httpReq = httpRequest(req)

    fountain.handle(httpReq)(resp)
  }
}
