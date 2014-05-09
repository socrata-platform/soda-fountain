package com.socrata.soda.server.errors

import com.rojoma.json.ast.{JObject, JValue}
import com.rojoma.json.util.AutomaticJsonCodecBuilder
import com.socrata.http.server.util.EntityTag
import javax.servlet.http.HttpServletResponse
import scala.{collection => sc}


abstract class SodaError(val httpResponseCode: Int, val errorCode: String, val data: Map[String, JValue]) {
  def this(httpResponseCode: Int, errorCode: String, data: (String, JValue)*) =
    this(httpResponseCode, errorCode, data.foldLeft(Map.empty[String, JValue]) { (acc, kv) =>
      assert(!acc.contains(kv._1), "Data field " + kv + " defined twice")
      acc + kv
    })

  def this(errorCode: String, data: (String, JValue)*) =
    this(HttpServletResponse.SC_BAD_REQUEST, errorCode, data : _*)

  def etags: Seq[EntityTag] = Nil
  def vary: Option[String] = None

  // Some responses, such as NotModified, cannot have content according to HTTP 1.1
  def hasContent: Boolean = true
  def humanReadableMessage: String = SodaError.translate(errorCode, data)
}

object SodaError {
  def translate(errcode: String, data: sc.Map[String, JValue]): String = errcode

  val QueryCoordinatorErrorCodec = AutomaticJsonCodecBuilder[QueryCoordinatorError]
}

case class QueryCoordinatorError(errorCode: String, data: JObject)
