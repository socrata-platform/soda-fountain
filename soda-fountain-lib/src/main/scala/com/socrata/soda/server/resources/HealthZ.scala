package com.socrata.soda.server.resources

import com.rojoma.json.v3.ast.{JBoolean, JObject}
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.soda.clients.regioncoder.RegionCoderClient
import com.socrata.soda.clients.regioncoder.RegionCoderClient._
import com.rojoma.json.v3.codec.JsonEncode

case class HealthZ(regionCoder: RegionCoderClient) {
  val log = org.slf4j.LoggerFactory.getLogger(getClass)

  private def logVersionError(url: String, status: Int, response: String): Unit =
    log.error(s"Unexpected response from region-coder @ $url: " +
      s"status $status, " +
      s"response: ${response.take(PartialResponseLength)}")

  object service extends SodaResource {
    override def get = { req => response =>
      val (code, fields) = regionCoder.versionCheck match {
        case Success =>
          (OK, Map("region_coder_ok" -> JBoolean.canonicalTrue))
        case f: Failure =>
          logVersionError(f.url, f.status, f.partialResponse)
          val details = JsonEncode.toJValue(f)
          (InternalServerError, Map("region_coder_ok" -> JBoolean.canonicalFalse, "details" -> details))
      }

      (code ~> Json(JObject(fields)))(response)
    }
  }
}
