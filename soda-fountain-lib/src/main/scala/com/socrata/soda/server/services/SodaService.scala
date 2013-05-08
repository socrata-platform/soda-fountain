package com.socrata.soda.server.services

import javax.servlet.http.HttpServletResponse
import com.socrata.http.server.responses._
import scala.Some
import com.rojoma.json.ast._
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.socrata.datacoordinator.client.DataCoordinatorClient
import com.socrata.soda.server.persistence.NameAndSchemaStore

trait SodaService {

  val dc : DataCoordinatorClient
  val store : NameAndSchemaStore

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
