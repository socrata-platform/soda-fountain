package com.socrata.soda.server.services

import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}
import dispatch._
import com.socrata.datacoordinator.client._
import com.rojoma.json.ast._
import com.rojoma.json.util.JsonUtil
import com.socrata.soda.server.types.TypeChecker
import java.io.IOException
import com.rojoma.json.io.JsonReaderException

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
          val response = dc.update(datasetId, schemaHash(request), mockUser, Array(DeleteRow(pkValue(rowId, schema))).iterator)
          passThroughResponse(response, start, "RowService.delete", resourceName, datasetId)
        }
      }
    }
    def get(resourceName: String, rowId: String)(request:HttpServletRequest): HttpServletResponse => Unit = {
      val start = System.currentTimeMillis
      if (!validName(resourceName)) { return sendInvalidNameError(resourceName, request)}
      withDatasetId(resourceName){ datasetId =>
        withDatasetSchema(datasetId) { schema =>
          val pkVal = pkValue(rowId, schema) match { case Left(s) => s"'${s}'"; case Right(n) => n.toString}
          val response = qc.query(datasetId, "select * where " + schema.pk + " = " + pkVal)
          passThroughResponse(response, start, "RowService.get", resourceName, datasetId)
        }
      }
    }
  }
}
