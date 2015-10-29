package com.socrata.soda.server

import java.nio.charset.{Charset, StandardCharsets}
import javax.servlet.http.HttpServletRequest

import com.rojoma.json.v3.codec._
import com.rojoma.json.v3.io.CompactJsonWriter
import com.socrata.http.server.HttpResponse
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.util._
import com.socrata.http.server.util.RequestId.{ReqIdHeader, RequestId}
import com.socrata.soda.server.errors.{InternalException, SodaError}
import com.socrata.soda.server.id.{AbstractId, ResourceName}
import com.socrata.soql.environment.AbstractName

object SodaUtils {
  val errorLog = org.slf4j.LoggerFactory.getLogger("com.socrata.soda.server.Error")

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

  @deprecated(message = "Need to negotiate a charset", since = "forever")
  def JsonContent[T: JsonEncode : JsonDecode](thing: T): HttpResponse =
    JsonContent(thing, StandardCharsets.UTF_8)

  def errorResponse(req: HttpServletRequest, error: SodaError, logTags: LogTag*): HttpResponse = {
    import com.rojoma.json.v3.ast._

    errorLog.info(s"${logTags.mkString(" ")} responding with error ${error.errorCode}: ${error.humanReadableMessage}")
    val header = error.vary.foldLeft(error.etags.foldLeft(Status(error.httpResponseCode)) { (h, et) =>
      h ~> ETag(et)
    }) { (h, vary) =>
      h ~> Header("Vary", vary)
    }

    def potentialContent =
      if (error.hasContent) {
        // TODO: Content-type and language negotiation
        val content = JObject(Map(
          "message" -> JString(error.humanReadableMessage),
          "errorCode" -> JString(error.errorCode),
          "data" -> JObject(error.sanitizedData)
        ))
        Json(content)
      } else {
        // when passing unit, must pass using (()) as compiler will not infer () you meaning to pass Unit.
        Function.const(()) _
      }

    header ~> potentialContent
  }

  def internalError(request: HttpServletRequest, th: Throwable, logTags: LogTag*): HttpResponse = {
    val tag = java.util.UUID.randomUUID.toString
    errorLog.error("Internal exception: " + tag, th)
    errorResponse(request, InternalException(th, tag), logTags: _*)
  }

  /**
   * Creates a bunch of standard headers to send to DC and QC for request tracing
   */
  def traceHeaders(requestId: RequestId, dataset: ResourceName): Map[String, String] =
    Map(ReqIdHeader -> requestId,
        ResourceHeader -> dataset.name)

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
