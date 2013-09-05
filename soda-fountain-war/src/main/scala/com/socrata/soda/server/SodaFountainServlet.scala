package com.socrata.soda.server

import javax.servlet.http.{HttpServletResponse, HttpServletRequest, HttpServlet}
import com.typesafe.config.ConfigFactory
import com.socrata.soda.server.config.SodaFountainConfig

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
    fountain.handle(req)(resp)
  }
}
