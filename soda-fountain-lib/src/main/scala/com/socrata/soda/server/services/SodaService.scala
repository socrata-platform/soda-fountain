package com.socrata.soda.server.services

import javax.servlet.http.{HttpServletResponse, HttpServletRequest}
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._

class SodaService {

}

object SodaService {
  def get(request:HttpServletRequest) : HttpServletResponse => Unit = {
    OK ~> ContentType("application/json; charset=utf-8") ~> Content("{response:'ok'}")
  }
}
