package com.socrata.soda.server.services

import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}
import dispatch._
import com.socrata.datacoordinator.client._
import com.rojoma.json.ast._
import com.rojoma.json.util.JsonUtil
import java.io.{StringReader, IOException}
import com.rojoma.json.io.JsonReaderException
import com.socrata.soda.server.services.ClientRequestExtractor._

trait RowService extends SodaService {

  object rows {

    val log = org.slf4j.LoggerFactory.getLogger(classOf[RowService])

    def upsert(resourceName: String, rowId: String)(request:HttpServletRequest): HttpServletResponse => Unit = {
      val start = System.currentTimeMillis
      if (!validName(resourceName)) { return sendInvalidNameError(resourceName, request)}

      jsonSingleObjectStream(request, MAX_DATUM_SIZE) match {
        case Right(obj) =>
          prepareForUpsert(resourceName, Iterator.single(obj), "row.upsert"){ (datasetId, schema, upserts) =>
            val response = dc.update(datasetId, schemaHash(request), mockUser, Array(UpsertRow(obj.fields)).iterator)
            passThroughResponse(response, start, "row.upsert", resourceName, datasetId)
          }
        case Left(err) => return sendErrorResponse("Error reading JSON: " + err, "row.upsert.json.iterator.error", BadRequest, None, resourceName)
      }
    }

    def delete(resourceName: String, rowId: String)(request:HttpServletRequest): HttpServletResponse => Unit = {
      val start = System.currentTimeMillis
      if (!validName(resourceName)) { return sendInvalidNameError(resourceName, request)}
      withDatasetId(resourceName){ datasetId =>
        withDatasetSchema(datasetId) { schema =>
          val r = dc.update(datasetId, schemaHash(request), mockUser, Array(DeleteRow(pkValue(rowId, schema))).iterator)
          val logTags = Seq("row.delete", resourceName, datasetId, rowId)
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
                    sendErrorResponse("row not found", "row.get.not-found", NotFound, None, logTags:_*)
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
