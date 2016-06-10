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

  def user(req: HttpRequest): String = {
    val fromHeader = for {
      authenticate <- req.header("Authenticate")
      if authenticate.startsWith("Basic ")
    } yield new String(Base64.decodeBase64(authenticate.split(" ", 1)(1)), "latin1").split(":")(0)
    fromHeader.getOrElse {
      SodaResource.log.info("Unable to get user out of Authenticate header; going with `anonymous' for now")
      "anonymous"
    }
  }

  def optionalHeader(header: String, headerValue: Option[String]): HttpResponse =
    headerValue match {
      case Some(v) => Header(header, v)
      case None => Function.const(())
    }
}

object SodaResource {
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[SodaResource])
}
