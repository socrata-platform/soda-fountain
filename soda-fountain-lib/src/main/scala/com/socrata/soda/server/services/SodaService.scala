package com.socrata.soda.server.services

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import com.socrata.http.server.{responses, HttpResponse}
import scala.Some
import com.rojoma.json.ast._
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.socrata.datacoordinator.client.DataCoordinatorClient
import com.socrata.soda.server.persistence.NameAndSchemaStore
import com.socrata.querycoordinator.client.QueryCoordinatorClient
import dispatch._
import com.typesafe.config.ConfigFactory
import com.socrata.datacoordinator.client.DataCoordinatorClient.SchemaSpec
import com.ning.http.client.Response

object SodaService {
  val config = ConfigFactory.load().getConfig("com.socrata.soda-fountain")
}

trait SodaService {

  val dc : DataCoordinatorClient
  val store : NameAndSchemaStore
  val qc : QueryCoordinatorClient
  val mockUser = "soda-server-community-edition"
  //val log = org.slf4j.LoggerFactory.getLogger(classOf[SodaService])

  def schemaHash(r: HttpServletRequest) = Option(r.getParameter("schema"))

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

  def passThroughResponse(f: Future[Either[Throwable,Response]]): HttpServletResponse => Unit = {
    f() match {
      case Right(response) => passThroughResponse(response)
      case Left(th) => sendErrorResponse(th.getMessage, "internal.error", InternalServerError, None)
    }
  }
  def passThroughResponse(response: Response): HttpServletResponse => Unit = {
    responses.Status(response.getStatusCode) ~>  ContentType(response.getContentType) ~> Content(response.getResponseBody)
  }

  def pkValue(rowId: String, schema: SchemaSpec) = {
    val pkType = schema.schema.get(schema.pk).getOrElse(throw new Exception("Primary key column not represented in schema. This should not happen."))
    pkType match { case "text" => Left(rowId); case "number" => Right(BigDecimal(rowId)); case _ => throw new Exception("Primary key column not text or number")}
  }

  def notSupported(id:String)(request:HttpServletRequest): HttpServletResponse => Unit = ???
  //surely this can be improved, but changing it to a String* vararg makes the router angry.
  def notSupported2(id:String, part2:String)(request:HttpServletRequest): HttpServletResponse => Unit = ???

  def withDatasetId(resourceName: String)(f: String => HttpResponse): HttpResponse = {
    val rnf = store.translateResourceName(resourceName)
    rnf() match {
      case Right(datasetId) => f(datasetId)
      case Left(err) => sendErrorResponse(err, "dataset.not.found", BadRequest, None)
    }
  }
  def withDatasetSchema(datasetId: String)(f: SchemaSpec => HttpResponse): HttpResponse = {
    val sf = dc.getSchema(datasetId)
    sf() match {
      case Right(schema) => f(schema)
      case Left(err) => sendErrorResponse(err, "schema.not-found", NotFound, None)
    }
  }
}
