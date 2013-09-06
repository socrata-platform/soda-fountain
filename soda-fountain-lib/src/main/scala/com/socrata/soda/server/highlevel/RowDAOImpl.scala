package com.socrata.soda.server.highlevel

import com.socrata.soda.server.id.{ColumnId, ResourceName}
import com.socrata.soda.server.highlevel.RowDAO._
import com.socrata.soda.server.persistence.NameAndSchemaStore
import com.socrata.soda.clients.querycoordinator.QueryCoordinatorClient
import com.socrata.soda.clients.datacoordinator.DataCoordinatorClient
import com.socrata.soql.environment.ColumnName

class RowDAOImpl(store: NameAndSchemaStore, dc: DataCoordinatorClient, qc: QueryCoordinatorClient) extends RowDAO {
  def query(resourceName: ResourceName, query: String): Result = {
    store.translateResourceName(resourceName) match {
      case Some((datasetId, schemaLite)) =>
        assert(!schemaLite.contains(ColumnName(":id")), "I know about system columns now; update the RowDAOImpl to avoid special-casing them")
        val schemaWithSystemColumns = schemaLite ++ systemSchema

        val (code, response) = qc.query(datasetId, query, schemaWithSystemColumns)
        Success(code, response) // TODO: Gah I don't even know where to BEGIN listing the things that need doing here!
      case None =>
        NotFound(resourceName)
    }
  }

  private[this] val systemSchema = Seq(
    ColumnName(":id") -> new ColumnId(":id"),
    ColumnName(":created_at") -> new ColumnId(":created_at"),
    ColumnName(":updated_at") -> new ColumnId(":updated_at")
  )
}
