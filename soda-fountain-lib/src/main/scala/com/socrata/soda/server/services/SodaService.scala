package com.socrata.soda.server.services

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import com.socrata.http.server.responses._
import scala.Some
import com.rojoma.json.ast._
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.socrata.datacoordinator.client.DataCoordinatorClient
import com.socrata.soda.server.persistence.NameAndSchemaStore
import com.socrata.querycoordinator.client.QueryCoordinatorClient

trait SodaService {

  val dc : DataCoordinatorClient
  val store : NameAndSchemaStore
  val qc : QueryCoordinatorClient
  val mockUser = "soda-server-community-edition"

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

  def notSupported(id:String)(request:HttpServletRequest): HttpServletResponse => Unit = ???
  //surely this can be improved, but changing it to a String* vararg makes the router angry.
  def notSupported2(id:String, part2:String)(request:HttpServletRequest): HttpServletResponse => Unit = ???
}
