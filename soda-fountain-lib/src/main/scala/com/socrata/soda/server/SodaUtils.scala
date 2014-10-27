package com.socrata.soda.server

import com.rojoma.json.ast.JValue
import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.io.CompactJsonWriter
import com.socrata.http.server.HttpResponse
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.HttpMethods
import com.socrata.http.server.util._
import com.socrata.http.server.util.RequestId.{RequestId, ReqIdHeader}
import com.socrata.soda.server.errors.{EtagPreconditionFailed, ResourceNotModified, InternalError,
                                       InternalException, SodaError}
import com.socrata.soda.server.id.{AbstractId, ResourceName}
import com.socrata.soql.environment.AbstractName
import java.nio.charset.{StandardCharsets, Charset}
import javax.servlet.http.HttpServletRequest

object SodaUtils {
  val errorLog = org.slf4j.LoggerFactory.getLogger("com.socrata.soda.server.Error")

  val jsonContentTypeBase = "application/json"
  val jsonContentTypeUtf8 = jsonContentTypeBase + "; charset=utf-8"

  val ResourceHeader = "X-Socrata-Resource"

  def JsonContent[T : JsonCodec](thing: T, charset: Charset): HttpResponse = {
    val j = JsonCodec[T].encode(thing)
    ContentType(jsonContentTypeBase + "; charset=" + charset.name) ~> Write { out =>
      val jw = new CompactJsonWriter(out)
      jw.write(j)
      out.write('\n')
    }
  }

  @deprecated(message = "Need to negotiate a charset", since = "forever")
  def JsonContent[T : JsonCodec](thing: T): HttpResponse =
    JsonContent(thing, StandardCharsets.UTF_8)

  def errorResponse(req: HttpServletRequest, error: SodaError, logTags: LogTag*): HttpResponse = {
    import com.rojoma.json.ast._

    errorLog.info(s"${logTags.mkString(" ")} responding with error ${error.errorCode}")
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
          "data" -> JObject(error.data)
        ))
        JsonContent(content)
      } else
        Function.const()_

    header ~> potentialContent
  }

  def internalError(request: HttpServletRequest, th: Throwable, logTags: LogTag*): HttpResponse = {
    val tag = java.util.UUID.randomUUID.toString
    errorLog.error("Internal exception: " + tag, th)
    errorResponse(request, InternalException(th, tag), logTags:_*)
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
  implicit def s2tag(s: String) = new LogTag(s)
  implicit def n2tag(n: AbstractName[_]) = new LogTag(n.name)
  implicit def id2tag(i: AbstractId) = new LogTag(i.underlying)
}
