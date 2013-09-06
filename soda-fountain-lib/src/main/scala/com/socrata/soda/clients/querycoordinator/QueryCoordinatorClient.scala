package com.socrata.soda.clients.querycoordinator

import com.socrata.http.client.{RequestBuilder, HttpClient}
import com.socrata.soda.server.id.{ColumnId, DatasetId}
import com.socrata.soql.environment.ColumnName
import com.rojoma.json.ast.JValue
import com.rojoma.json.util.JsonUtil

trait QueryCoordinatorClient {
  def qchost : Option[RequestBuilder]
  val internalHttpClient : HttpClient

  private[this] val log = org.slf4j.LoggerFactory.getLogger(classOf[QueryCoordinatorClient])

  def query(datasetId: DatasetId, query: String, columnIdMap: Map[ColumnName, ColumnId]): (Int, JValue) =
    qchost match {
      case Some(host) =>
        val jsonizedColumnIdMap = JsonUtil.renderJson(columnIdMap.map { case(k,v) => k.name -> v.underlying})
        val request = host.form(List("ds" -> datasetId.underlying, "q" -> query, "idMap" -> jsonizedColumnIdMap))
        for (response <- internalHttpClient.execute(request)) yield {
          log.info("TODO: stream the response")
          (response.resultCode, response.asJValue())
        }
      case None => throw new Exception("could not connect to query coordinator")
    }
}
