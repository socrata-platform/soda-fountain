package com.socrata.soda.server.responses

import javax.servlet.http.HttpServletResponse

import com.rojoma.json.v3.ast.{JObject, JValue}
import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder
import com.socrata.http.server.util.EntityTag

import scala.{collection => sc}

abstract class SodaResponse(val httpResponseCode: Int, val responseCode: String, val message: String, val data: Map[String, JValue]) {
  def this(httpResponseCode: Int, responseCode: String, message: String, data: (String, JValue)*) =
    this(httpResponseCode, responseCode, message, data.foldLeft(Map.empty[String, JValue]) { (acc, kv) =>
      assert(!acc.contains(kv._1), "Data field " + kv + " defined twice")
      acc + kv
    })

  def this(responseCode: String, message: String, data: (String, JValue)*) =
    this(HttpServletResponse.SC_BAD_REQUEST, responseCode, message: String, data: _*)

  def etags: Seq[EntityTag] = Nil

  def vary: Option[String] = None

  // Some responses, such as NotModified, cannot have content according to HTTP 1.1
  def hasContent: Boolean = true

  def humanReadableMessage: String = SodaResponse.translate(responseCode, message, sanitizedData)

  def sanitizedData: Map[String, JValue] = data -- excludedFields

  def excludedFields: Set[String] = Set.empty
}

object SodaResponse {
  // errcode gets separately logged in soda-fountain, core, and di2. message also incorporates appropriate things in data right now.
  def translate(responseCode: String, message: String, data: sc.Map[String, JValue]): String = message
}
