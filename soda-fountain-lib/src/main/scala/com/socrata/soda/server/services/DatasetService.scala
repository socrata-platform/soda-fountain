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
import java.io.Reader


trait DatasetService extends SodaService {

  object dataset {

    def upsert(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit = ???
    def replace(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit = ???
    def truncate(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit = ???
    def query(resourceName: String)(request:HttpServletRequest): HttpServletResponse => Unit =  {
      ImATeapot ~> ContentType("text/plain; charset=utf-8") ~> Content("resource request not implemented")
    }
    def create()(request:HttpServletRequest): HttpServletResponse => Unit =  {
      DatasetSpec(request.getReader) match {
        case Right(dspec) => {
          val columnInstructions = dspec.columns.map(c => new AddColumnInstruction(c.name, c.dataType))
          val instructions = dspec.rowId match{
            case Some(rid) => columnInstructions :+ SetRowIdColumnInstruction(rid)
            case None => columnInstructions
          }
          val r = dc.create(dspec.resourceName, mockUser, Some(instructions), dspec.locale )
          r() match {
            case Right((datasetId, records)) => {
              store.store(dspec.resourceName, datasetId)  // TODO: handle failure here, see list of errors from DC.
              OK
            }
            case Left(thr) => sendErrorResponse("could not create dataset", "internal.error", InternalServerError, Some(JString(thr.getMessage)))
          }
        }
        case Left(ers: Seq[String]) => sendErrorResponse( ers.mkString(" "), "dataset.specification.invalid", BadRequest, None )
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
