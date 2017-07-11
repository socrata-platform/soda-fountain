package com.socrata.soda.server.resources

import com.socrata.http.server.routing.SimpleResource
import com.socrata.http.server.{HttpRequest, HttpResponse, HttpService}
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.soda.server._
import com.socrata.soda.server.SodaUtils
import com.socrata.soda.server.responses.HttpMethodNotAllowed
import org.apache.commons.codec.binary.Base64

class SodaResource extends SimpleResource {

  override def methodNotAllowed: HttpService = { req =>
    val allowed = allowedMethods
    Header("Allow", allowed.mkString(",")) ~> SodaUtils.response(
      req,
      HttpMethodNotAllowed(req.getMethod, allowed))
  }

  // SF doesn't handle auth, unlike what was originally envisionsed, but is still wired up to pass user info down
  // to DC, this could be used for debugging or ripped out.
  def user(req: HttpRequest): String = "anonymous"

  def optionalHeader(header: String, headerValue: Option[String]): HttpResponse =
    headerValue match {
      case Some(v) => Header(header, v)
      case None => Function.const(())
    }
}

object SodaResource {
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[SodaResource])
}
