package com.socrata.soda.clients.querycoordinator

import com.socrata.http.client.{Response, RequestBuilder, HttpClient}
import com.socrata.soda.server.id.{ColumnId, DatasetId}
import com.socrata.soql.environment.ColumnName
import com.socrata.soda.clients.querycoordinator.QueryCoordinatorClient._
import com.rojoma.json.util.JsonUtil
import com.socrata.http.server.util._
import com.socrata.http.server.implicits._
import org.apache.http.HttpStatus
import scala.io.{Codec, Source}
import org.joda.time.DateTime

trait HttpQueryCoordinatorClient extends QueryCoordinatorClient {
  def qchost : Option[RequestBuilder]
  val httpClient: HttpClient

  private[this] val log = org.slf4j.LoggerFactory.getLogger(classOf[HttpQueryCoordinatorClient])

  private val qpDataset = "ds"
  private val qpQuery = "q"
  private val qpIdMap = "idMap"
  private val qpRowCount = "rowCount"
  private val secondaryStoreOverride = "store"

  def query[T](datasetId: DatasetId, precondition: Precondition, ifModifiedSince: Option[DateTime], query: String, columnIdMap: Map[ColumnName, ColumnId], rowCount: Option[String], secondaryInstance:Option[String])(f: Result => T): T = {
    import HttpStatus._

    def resultFrom(response: Response): Result = {
      response.resultCode match {
        case SC_OK =>
          Success(response.headers("ETag").map(EntityTagParser.parse(_)), response.asJValue())
        case SC_NOT_MODIFIED =>
          NotModified(response.headers("ETag").map(EntityTagParser.parse(_)))
        case SC_PRECONDITION_FAILED =>
          PreconditionFailed
        case code if code >= SC_BAD_REQUEST && code < SC_INTERNAL_SERVER_ERROR =>
          UserError(code, response.asJValue())
        case _ =>
          val body = if (response.isJson) response.asJValue().toString() else Source.fromInputStream(response.asInputStream())(Codec(response.charset)).mkString("")
          throw new Exception(s"query: ${query}; response: ${body}")
      }
    }

    qchost match {
      case Some(host) =>
        val jsonizedColumnIdMap = JsonUtil.renderJson(columnIdMap.map { case(k,v) => k.name -> v.underlying})
        val params = List(qpDataset -> datasetId.underlying, qpQuery -> query, qpIdMap -> jsonizedColumnIdMap) ++
          rowCount.map(rc => List(qpRowCount -> rc)).getOrElse(Nil) ++
          secondaryInstance.map(so => List(secondaryStoreOverride -> so)).getOrElse(Nil)
        log.info("Query Coordinator request parameters: " + params)
        val request = host.addHeaders(PreconditionRenderer(precondition) ++ ifModifiedSince.map("If-Modified-Since" -> _.toHttpDate)).form(params)
        for (response <- httpClient.execute(request)) yield {
          log.info("TODO: stream the response")
          f(resultFrom(response))
        }
      case None => throw new Exception("could not connect to query coordinator")
    }
  }
}
