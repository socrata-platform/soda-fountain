package com.socrata.soda.clients.querycoordinator

import com.socrata.soda.server.id.{ColumnId, DatasetId}
import com.socrata.soql.environment.ColumnName
import com.rojoma.json.ast.JValue

trait QueryCoordinatorClient {
  def query(datasetId: DatasetId, query: String, columnIdMap: Map[ColumnName, ColumnId]): (Int, JValue)
}
