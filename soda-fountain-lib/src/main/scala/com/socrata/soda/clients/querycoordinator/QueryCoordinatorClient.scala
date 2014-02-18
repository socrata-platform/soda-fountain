package com.socrata.soda.clients.querycoordinator

import com.socrata.soda.server.id.{ColumnId, DatasetId}
import com.socrata.soql.environment.ColumnName
import com.rojoma.json.ast.JValue
import com.socrata.http.server.util.{Precondition, EntityTag}

trait QueryCoordinatorClient {
  def query(datasetId: DatasetId, precondition: Precondition, query: String, columnIdMap: Map[ColumnName, ColumnId], rowCount: Option[String], secondaryInstance:Option[String]): (Int, Seq[EntityTag], JValue)
}
