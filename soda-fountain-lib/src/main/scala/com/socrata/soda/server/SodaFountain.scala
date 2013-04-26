package com.socrata.soda.server

import com.rojoma.json.ast._
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import javax.servlet.http.HttpServletResponse

object SodaFountain {

  def sendErrorResponse(message: String, errorCode: String, httpCode: HttpServletResponse => Unit = BadRequest, data: Option[JValue] = None) = {
    val messageAndCode = Map[String, JValue](
      "message" -> JString(message),
      "errorCode" -> JString(errorCode)
    )
    val errorMap = data match {
      case Some(d) => messageAndCode + ("data" -> d)
      case None => messageAndCode
    }
    httpCode ~> ContentType("application/json; charset=utf-8") ~> Content(JObject(errorMap).toString)
  }
}
