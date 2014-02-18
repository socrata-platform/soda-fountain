package com.socrata.soda.clients.querycoordinator

import com.socrata.http.client.{RequestBuilder, HttpClient}
import com.socrata.soda.server.id.{ColumnId, DatasetId}
import com.socrata.soql.environment.ColumnName
import com.rojoma.json.ast.JValue
import com.rojoma.json.util.JsonUtil
import scala.io.{Codec, Source}
import com.socrata.http.server.util._

abstract class HttpQueryCoordinatorClient(httpClient: HttpClient) extends QueryCoordinatorClient {
  def qchost : Option[RequestBuilder]

  private[this] val log = org.slf4j.LoggerFactory.getLogger(classOf[HttpQueryCoordinatorClient])

  private val qpDataset = "ds"
  private val qpQuery = "q"
  private val qpIdMap = "idMap"
  private val qpRowCount = "rowCount"
  private val secondaryStoreOverride = "store"

  def query(datasetId: DatasetId, precondition: Precondition, query: String, columnIdMap: Map[ColumnName, ColumnId], rowCount: Option[String], secondaryInstance:Option[String]): (Int, Seq[EntityTag], JValue) =
    qchost match {
      case Some(host) =>
        val jsonizedColumnIdMap = JsonUtil.renderJson(columnIdMap.map { case(k,v) => k.name -> v.underlying})
        val params = List(qpDataset -> datasetId.underlying, qpQuery -> query, qpIdMap -> jsonizedColumnIdMap) ++
          rowCount.map(rc => List(qpRowCount -> rc)).getOrElse(Nil) ++
          secondaryInstance.map(so => List(secondaryStoreOverride -> so)).getOrElse(Nil)
        log.info("Query Coordinator request parameters: " + params)
        val request = host.addHeaders(PreconditionRenderer(precondition)).form(params)
        for (response <- httpClient.execute(request)) yield {
          log.info("TODO: stream the response")
          response.resultCode match {
            case 200 =>
              (response.resultCode, response.headers("ETag").map(EntityTagParser.parse(_)), response.asJValue())
            case _ =>
              val body = if (response.isJson) response.asJValue().toString() else Source.fromInputStream(response.asInputStream())(Codec(response.charset)).mkString("")
              throw new Exception(s"query: ${query}; response: ${body}")
          }
        }
      case None => throw new Exception("could not connect to query coordinator")
    }
}
