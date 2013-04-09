package com.socrata.soda.server.services

import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._


object DatasetService {
  def getByResourceName = {
    ImATeapot ~> ContentType("text/plain; charset=utf-8") ~> Content("resource request not implemented")
  }

  def upsert = {

    ImATeapot ~> ContentType("text/plain; charset=utf-8") ~> Content("upsert request not implemented")
  }
}

class DatasetService {

}
