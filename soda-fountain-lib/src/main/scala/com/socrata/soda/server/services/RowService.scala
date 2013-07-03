package com.socrata.soda.server.services

import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}
import dispatch._
import com.socrata.datacoordinator.client._
import com.rojoma.json.ast._
import com.rojoma.json.util.JsonUtil
import com.socrata.soda.server.types.TypeChecker
import java.io.{StringReader, IOException}
import com.rojoma.json.io.JsonReaderException
import com.ning.http.client.Response
import com.socrata.http.server.responses
import com.socrata.datacoordinator.client.DataCoordinatorClient.RowOpReport

trait RowService extends SodaService {

  object rows {

    val log = org.slf4j.LoggerFactory.getLogger(classOf[RowService])

    def upsert(resourceName: String, rowId: String)(request:HttpServletRequest): HttpServletResponse => Unit = {
      val start = System.currentTimeMillis
      if (!validName(resourceName)) { return sendInvalidNameError(resourceName, request)}
      try {
        val fields = JsonUtil.readJson[Map[String,JValue]](request.getReader)
        fields match {
          case Some(map) => {
            withDatasetId(resourceName){ datasetId =>
              withDatasetSchema(datasetId){ schema =>
                map.foreach{ pair =>
                  val expectedTypeName = schema.schema.get(pair._1).getOrElse( return sendErrorResponse("no column " + pair._1, "row.upsert.column.notfound", BadRequest, Some(map), resourceName, datasetId))
                  TypeChecker.check(expectedTypeName, pair._2) match {
                    case Right(v) => v
                    case Left(msg) => return sendErrorResponse(msg, "row.upsert.type.error", BadRequest, Some(Map(pair)), resourceName, datasetId)
                  }
                }
                val response = dc.update(datasetId, schemaHash(request), mockUser, Array(UpsertRow(map)).iterator)
                passThroughResponse(response, start, "RowService.upsert", resourceName, datasetId)
              }
            }
          }
          case None => sendErrorResponse("could not parse request body as single JSON object", "row.upsert.json.notsingleobject", BadRequest, None, resourceName )
        }
      } catch {
        case e: IOException => sendErrorResponse("could not read request body: " + e.getMessage, "row.upsert.json.read.error", UnsupportedMediaType, None, resourceName)
        case e: JsonReaderException => sendErrorResponse("could not parse request body as JSON: " + e.getMessage, "row.upsert.json.parse.error", UnsupportedMediaType, None, resourceName)
      }
    }

    def delete(resourceName: String, rowId: String)(request:HttpServletRequest): HttpServletResponse => Unit = {
      val start = System.currentTimeMillis
      if (!validName(resourceName)) { return sendInvalidNameError(resourceName, request)}
      withDatasetId(resourceName){ datasetId =>
        withDatasetSchema(datasetId) { schema =>
          val r = dc.update(datasetId, schemaHash(request), mockUser, Array(DeleteRow(pkValue(rowId, schema))).iterator)
          val logTags = Seq("RowService.delete", resourceName, datasetId, rowId)
          r() match {
            case Right(response) => response.getStatusCode match {
              case 200 =>
                log.info(s"${logTags.mkString(" ")} took ${System.currentTimeMillis - start} returning 204 - No Content")
                NoContent
              case _ => sendErrorResponse("internal error during row delete", "row.delete.unsuccessful", InternalServerError, None, logTags:_*)
            }
            case Left(th) => sendErrorResponse(th, "internal error during row delete", "row.delete.internal.error", InternalServerError, None, logTags:_*)
          }
        }
      }
    }
    def get(resourceName: String, rowId: String)(request:HttpServletRequest): HttpServletResponse => Unit = {
      val startTime = System.currentTimeMillis
      if (!validName(resourceName)) { return sendInvalidNameError(resourceName, request)}
      withDatasetId(resourceName){ datasetId =>
        withDatasetSchema(datasetId) { schema =>
          val pkVal = pkValue(rowId, schema) match { case Left(s) => s"'${s}'"; case Right(n) => n.toString}
          val r = qc.query(datasetId, "select * where " + schema.pk + " = " + pkVal)
          val logTags = Seq("row.get", resourceName, datasetId)
          r() match {
            case Right(response) => response.getStatusCode match {
              case 200 => {
                val rows = JsonUtil.readJson[Seq[JValue]](new StringReader(response.getResponseBody)).get
                rows.headOption match {
                  case Some(head) => {
                    log.info(s"${logTags.mkString(" ")} took ${System.currentTimeMillis - startTime} returning 200 - OK")
                    OK ~>  ContentType(response.getContentType) ~> Content(head.toString)
                  }
                  case None => {
                    sendErrorResponse("row not found", "row.get.notfound", NotFound, None, logTags:_*)
                  }
                }
              }
              case _ => sendErrorResponse("error executing row query", "row.get.unsuccessful", InternalServerError, None, logTags:_*)
            }
            case Left(th) => sendErrorResponse(th, "internal error during row get", "row.get.internal.error", InternalServerError, None, logTags:_*)
          }
        }
      }
    }
  }
}
