package com.socrata.soda.server

import com.socrata.soda.server.errors.SodaError
import com.socrata.http.server.HttpResponse
import javax.servlet.http.HttpServletRequest
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.rojoma.json.ast.JValue
import com.rojoma.json.io.CompactJsonWriter
import com.socrata.soql.environment.AbstractName
import com.socrata.soda.server.id.AbstractId
import com.rojoma.json.codec.JsonCodec

object SodaUtils {
  val errorLog = org.slf4j.LoggerFactory.getLogger("com.socrata.soda.server.Error")

  val jsonContentType = "application/json; charset=utf-8"

  // Note: this does NOT set the content-type!  That's deliberate, because
  // the content-type depends on the charset, which needs to be negotiated.
  def JsonContent[T : JsonCodec](thing: T) = {
    val j = JsonCodec[T].encode(thing)
    Write { out =>
      val jw = new CompactJsonWriter(out)
      jw.write(j)
      out.write('\n')
    }
  }

  def errorResponse(req: HttpServletRequest, error: SodaError, logTags: LogTag*): HttpResponse = {
    import com.rojoma.json.ast._
    // TODO: Content-type and language negotiation
    val content = JObject(Map(
      "message" -> JString(error.humanReadableMessage),
      "errorCode" -> JString(error.errorCode),
      "data" -> JObject(error.data)
    ))

    errorLog.info(s"${logTags.mkString(" ")} responding with error ${error.errorCode}")
    Status(error.httpResponseCode) ~> ContentType(jsonContentType) ~> JsonContent(content)
  }

  def internalError(request: HttpServletRequest, th: Throwable, logTags: LogTag*): HttpResponse = {
    val tag = java.util.UUID.randomUUID.toString
    errorLog.error("Internal error: " + tag, th)
    errorResponse(request, InternalError(tag), logTags:_*)
  }
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
