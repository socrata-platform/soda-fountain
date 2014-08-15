package com.socrata.soda.server.resources

import com.rojoma.json.ast.{JBoolean, JObject}
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.soda.clients.geospace.GeospaceClient
import com.socrata.soda.server.SodaUtils

case class HealthZ(geospace: GeospaceClient) {
  val log = org.slf4j.LoggerFactory.getLogger(getClass)

  object service extends SodaResource {
    override def get = { req => response =>
      val geospaceOk = geospace.versionCheck
      val status = if (geospaceOk) OK else InternalServerError

      (status ~> SodaUtils.JsonContent(JObject(Map("geospace_ok" -> JBoolean(geospaceOk)))))(response)
    }
  }
}
