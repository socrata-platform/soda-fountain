package com.socrata.soda.clients.querycoordinator

import com.socrata.http.client.{RequestBuilder, HttpClient}
import com.socrata.soda.server.id.{ColumnId, DatasetId}
import com.socrata.soql.environment.ColumnName
import com.rojoma.json.ast.JValue

trait QueryCoordinatorClient {
  def qchost : Option[RequestBuilder]
  val internalHttpClient : HttpClient

  def query(datasetId: DatasetId, query: String, columnIdMap: Map[ColumnName, ColumnId]): JValue =
    qchost match {
      case Some(host) =>
        val request = host.addParameter("ds", datasetId.underlying).addParameter("q", query).get
        for (response <- internalHttpClient.execute(request)) yield {
          response.asJValue()
        }
      case None => throw new Exception("could not connect to query coordinator")
    }
}
