package com.socrata.soda.server.services

import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}
import dispatch._
import com.socrata.datacoordinator.client._
import com.rojoma.json.ast._
import com.rojoma.json.util.JsonUtil
import scala.collection.Map
import com.socrata.soql.types.SoQLType
import com.socrata.soql.environment.TypeName


trait DatasetService extends SodaService {

  object dataset {

    def upsert(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit = ???
    def replace(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit = ???
    def truncate(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit = ???
    def query(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit =  {
      ImATeapot ~> ContentType("text/plain; charset=utf-8") ~> Content("resource request not implemented")
    }
    def create()(request:HttpServletRequest): HttpServletResponse => Unit =  {
      try {
        val fields = JsonUtil.readJson[Map[String,JValue]](request.getReader)
        fields match {
          case Some(requestMap) => {
            val spec = for {
              JString(resourceName) <- requestMap.get("resource_name")
              JString(name) <- requestMap.get("name")
              JArray(columns) <- requestMap.get("columns")
            } yield (resourceName, name, columns, requestMap.get("description"), requestMap.get("row_identifier"), requestMap.get("locale"))
            spec match {
              case Some((resourceName, name, jColumns, oDesc, oRowId, oLoc)) => {
                val allColumns = jColumns.map{ jval =>
                  val fields = jval.asInstanceOf[JObject].toMap
                  val c = for {
                    JString(name) <- fields.get("name")
                    JString(fieldName) <- fields.get("field_name")
                    JString(dataTypeName) <- fields.get("datatype")
                  }
                  yield (name, fieldName, dataTypeName, fields.get("description").map(_.asInstanceOf[JString].string) ) //TODO: catch the case that the description is not a string
                  c match {
                    case Some(c) => Right(c)
                    case None => Left(jval)
                  }
                }
                val badColumns = for (Left(bc) <- allColumns) yield bc
                if (badColumns.length == 0){
                  val goodColumns = for (Right(gc) <- allColumns) yield gc
                  val columnInstructions = goodColumns.map(c => new AddColumnInstruction(c._2, c._3))
                  val instructions = oRowId match{ case Some(rid) => columnInstructions :+ SetRowIdColumnInstruction(rid.asInstanceOf[JString].string); case None => columnInstructions}  //TODO: catch the bad cast
                  val loc = oLoc match { case Some(JString(loc)) => loc; case _ => "en_US"}
                  val r = dc.create(resourceName, mockUser, Some(instructions), loc )
                  r() match {
                    case Right((datasetId, records)) => {
                      store.store(resourceName, datasetId)  // TODO: handle failure here, see list of errors from DC.
                      OK
                    }
                    case Left(thr) => sendErrorResponse("could not create dataset", "internal.error", InternalServerError, Some(JString(thr.getMessage)))
                  }
                }
                else {
                  sendErrorResponse("name, field_name, and datatype are required for each column", "invalid.column", BadRequest, Some(JArray(badColumns)))
                }
              }
              case None => sendErrorResponse("resource_name, name, and columns must all be specified to create a dataset", "missing.values", BadRequest, None)
            }
          }
          case None => sendErrorResponse("could not parse request body for dataset creation", "parse.error", BadRequest, None)
        }
      } catch {
        case e: Exception => sendErrorResponse("could not parse request body as JSON: " + e.getMessage, "parse.error", UnsupportedMediaType, None)
        case _: Throwable => sendErrorResponse("error processing request", "internal.error", InternalServerError, None)
      }

    }
    def setSchema(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit =  {
      ImATeapot ~> ContentType("text/plain; charset=utf-8") ~> Content("resource request not implemented")
    }
    def getSchema(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit =  {
      ImATeapot ~> ContentType("text/plain; charset=utf-8") ~> Content("resource request not implemented")
    }
    def delete(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit = {
      ImATeapot ~> ContentType("text/plain; charset=utf-8") ~> Content("delete request not implemented")
    }
  }
}

