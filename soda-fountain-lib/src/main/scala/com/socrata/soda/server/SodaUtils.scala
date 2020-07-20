package com.socrata.soda.server

import java.net.URLEncoder
import java.nio.charset.{Charset, StandardCharsets}
import javax.servlet.http.HttpServletRequest

import com.rojoma.json.v3.codec._
import com.rojoma.json.v3.io.CompactJsonWriter
import com.socrata.http.server.{HttpRequest, HttpResponse}
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.util._
import com.socrata.http.server.util.RequestId.{ReqIdHeader, RequestId}
import com.socrata.soda.server.responses.{SodaInvalidRequest, InternalException, SodaResponse}
import com.socrata.soda.server.id.{AbstractId, ResourceName}
import com.socrata.soql.environment.AbstractName

object SodaUtils {
  val responseLog = org.slf4j.LoggerFactory.getLogger("com.socrata.soda.server.Response")

  val jsonContentTypeBase = "application/json"
  val jsonContentTypeUtf8 = jsonContentTypeBase + "; charset=utf-8"

  val ResourceHeader = "X-Socrata-Resource"

  def JsonContent[T: JsonEncode : JsonDecode](thing: T, charset: Charset): HttpResponse = {
    val j = JsonEncode[T].encode(thing)
    Write(jsonContentTypeBase + "; charset=" + charset.name) { out =>
      val jw = new CompactJsonWriter(out)
      jw.write(j)
      out.write('\n')
    }
  }

  def response(req: HttpRequest, resp: SodaResponse, logTags: LogTag*): HttpResponse = {
    response(req.servletRequest, resp, logTags : _*)
  }

  def response(req: HttpServletRequest, response: SodaResponse, logTags: LogTag*): HttpResponse = {
    import com.rojoma.json.v3.ast._

    responseLog.info(s"${logTags.mkString(" ")} responding with response ${response.responseCode}: ${response.humanReadableMessage}")
    val header = response.vary.foldLeft(response.etags.foldLeft(Status(response.httpResponseCode): HttpResponse) { (h, et) =>
      h ~> ETag(et)
    }) { (h, vary) =>
      h ~> Header("Vary", vary)
    }

    def potentialContent =
      if (response.hasContent) {
        // TODO: Content-type and language negotiation
        val content = JObject(Map(
          "message" -> JString(response.humanReadableMessage),
          // Returning as "errorCode" for backwards compatibility
          "errorCode" -> JString(response.responseCode),
          "data" -> JObject(response.sanitizedData)
        ))
        Json(content)
      } else {
        // when passing unit, must pass using (()) as compiler will not infer () you meaning to pass Unit.
        Function.const(()) _
      }

    header ~> potentialContent
  }

  def internalError(req: HttpRequest, th: Throwable, logTags: LogTag*): HttpResponse = {
    internalError(req.servletRequest, th, logTags : _*)
  }

  def internalError(request: HttpServletRequest, th: Throwable, logTags: LogTag*): HttpResponse = {
    val tag = java.util.UUID.randomUUID.toString
    responseLog.error("Internal exception: " + tag, th)
    response(request, InternalException(th, tag), logTags: _*)
  }

  def invalidRequest(req: HttpRequest, th: SodaInvalidRequestException, logTags: LogTag*): HttpResponse = {
    invalidRequest(req.servletRequest, th, logTags : _*)
  }

  def invalidRequest(request: HttpServletRequest, th: SodaInvalidRequestException, logTags: LogTag*): HttpResponse = {
    val tag = java.util.UUID.randomUUID.toString
    responseLog.error("Invalid request: " + tag)
    response(request, SodaInvalidRequest(th, tag), logTags: _*)
  }

  def handleError(req: HttpRequest, th: Throwable, logTags: LogTag*): HttpResponse = {
    handleError(req.servletRequest, th, logTags : _*)
  }

  def handleError(request: HttpServletRequest, th: Throwable, logTags: LogTag*): HttpResponse = {
    th match {
      case sir: SodaInvalidRequestException => invalidRequest(request, sir, logTags: _*)
      case sie: SodaInternalException => internalError(request, sie, logTags: _*) // TODO do we want to include these messages? too detailed?
      case e => internalError(request, e, logTags: _*)
    }
  }

  /**
   * Creates a bunch of standard headers to send to DC and QC for request tracing
   */
  def traceHeaders(requestId: RequestId, dataset: ResourceName): Map[String, String] =
    Map(ReqIdHeader -> requestId,
        ResourceHeader -> URLEncoder.encode(dataset.name, "UTF-8"))

}

class LogTag(s: String) {
  override def toString = s
}

object LogTag {
  import scala.language.implicitConversions
  implicit def s2tag(s: String): LogTag = new LogTag(s)
  implicit def n2tag(n: AbstractName[_]): LogTag = new LogTag(n.name)
  implicit def id2tag(i: AbstractId): LogTag = new LogTag(i.underlying)
}
