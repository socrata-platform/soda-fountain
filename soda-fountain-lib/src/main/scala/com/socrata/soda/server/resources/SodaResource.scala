package com.socrata.soda.server.resources

import com.socrata.http.server.routing.SimpleResource
import com.socrata.http.server.{HttpResponse, HttpService}
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.soda.server.SodaUtils
import com.socrata.soda.server.errors.HttpMethodNotAllowed
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}
import org.apache.commons.codec.binary.Base64
import org.joda.time.format.DateTimeFormat

class SodaResource extends SimpleResource {

  val HttpDateFormat = DateTimeFormat.forPattern("E, dd MMM YYYY HH:mm:ss 'GMT'").withZoneUTC

  override def methodNotAllowed: HttpService = { req =>
    val allowed = allowedMethods
    Header("Allow", allowed.mkString(",")) ~> SodaUtils.errorResponse(
      req,
      HttpMethodNotAllowed(req.getMethod, allowed))
  }

  def user(req: HttpServletRequest): String = {
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
