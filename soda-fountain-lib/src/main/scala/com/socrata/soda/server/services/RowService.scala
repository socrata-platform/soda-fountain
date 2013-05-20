package com.socrata.soda.server.services

import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}
import dispatch._
import com.socrata.datacoordinator.client._
import com.rojoma.json.ast._
import com.rojoma.json.util.JsonUtil

trait RowService extends SodaService {

  object rows {

    def upsert(resourceName: String, rowId: String)(request:HttpServletRequest): HttpServletResponse => Unit = {
      try {
        val fields = JsonUtil.readJson[Map[String,JValue]](request.getReader)
        fields match {
          case Some(map) => {
            val rnf = store.translateResourceName(resourceName)
            rnf() match {
              case Right(datasetId) => {
                val response = dc.update(datasetId, None, "soda-fountain-community-edition", Array(UpsertRow(map)).iterator)
                response() match {
                  case Right(resp) => DataCoordinatorClient.passThroughResponse(resp)
                  case Left(th) => sendErrorResponse(th.getMessage, "internal.error", InternalServerError, None)
                }
              }
              case Left(err) => sendErrorResponse(err, "unknown.dataset", NotFound, Some(JString(resourceName)))
            }
          }
          case None => sendErrorResponse("could not parse request body as single JSON object", "parse.error", BadRequest, None)
        }
      } catch {
        case e: Exception => sendErrorResponse("could not parse request body as JSON: " + e.getMessage, "parse.error", UnsupportedMediaType, None)
        case _: Throwable => sendErrorResponse("error processing request", "internal.error", InternalServerError, None)
      }
    }

    def delete(resourceName: String, rowId: String)(request:HttpServletRequest): HttpServletResponse => Unit = ???
    def get(resourceName: String, rowId: String)(request:HttpServletRequest): HttpServletResponse => Unit = ???
  }
}
