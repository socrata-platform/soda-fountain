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
                  val expectedTypeName = schema.schema.get(pair._1).getOrElse( return sendErrorResponse("no column " + pair._1, "column.not.found", BadRequest, Some(JObject(map)), "RowService.upsert", resourceName, datasetId))
                  TypeChecker.check(expectedTypeName, pair._2) match {
                    case Right(v) => v
                    case Left(msg) => return sendErrorResponse(msg, "type.error", BadRequest, Some(pair._2), "RowService.upsert", resourceName, datasetId)
                  }
                }
                val response = dc.update(datasetId, schemaHash(request), mockUser, Array(UpsertRow(map)).iterator)
                passThroughResponse(response, start, "RowService.upsert", resourceName, datasetId)
              }
            }
          }
          case None => sendErrorResponse("could not parse request body as single JSON object", "parse.error", BadRequest, None, "RowService.upsert", resourceName )
        }
      } catch {
        case e: IOException => sendErrorResponse("could not read request body: " + e.getMessage, "parse.error", UnsupportedMediaType, None, "RowService.upsert", resourceName)
        case e: JsonReaderException => sendErrorResponse("could not parse request body as JSON: " + e.getMessage, "parse.error", UnsupportedMediaType, None, "RowService.upsert", resourceName)
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
              case _ => sendErrorResponse("error executing row delete", "row.delete.internal.error", InternalServerError, None, logTags:_*)
            }
            case Left(th) => sendErrorResponse(th.getMessage, "internal.error", InternalServerError, None, logTags:_*)
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
          val logTags = Seq("RowService.get", resourceName, datasetId)
          r() match {
            case Right(response) => response.getStatusCode match {
              case 200 => {
                val oRows = JsonUtil.readJson[Seq[JValue]](new StringReader(response.getResponseBody))
                val rows = oRows.getOrElse{ return sendErrorResponse("error executing row query", "row.query.internal.error", InternalServerError, None, logTags:_*)  }
                rows.headOption match {
                  case Some(head) => {
                    log.info(s"${logTags.mkString(" ")} took ${System.currentTimeMillis - startTime} returning 200 - OK")
                    OK ~>  ContentType(response.getContentType) ~> Content(head.toString)
                  }
                  case None => {
                    log.info(s"${logTags.mkString(" ")} took ${System.currentTimeMillis - startTime} returning Not Found - 404")
                    sendErrorResponse("row not found", "row.not.found", NotFound, None, logTags:_*)
                  }
                }
              }
              case _ => sendErrorResponse("error executing row query", "row.query.internal.error", InternalServerError, None, logTags:_*)
            }
            case Left(th) => sendErrorResponse(th.getMessage, "internal.error", InternalServerError, None, logTags:_*)
          }
        }
      }
    }
  }
}
