package com.socrata.soda.server.highlevel

import com.socrata.soda.server.id.ResourceName
import com.socrata.soda.server.highlevel.RowDAO._
import com.socrata.soda.server.persistence.NameAndSchemaStore
import com.socrata.soda.clients.querycoordinator.QueryCoordinatorClient
import com.socrata.soda.clients.datacoordinator.DataCoordinatorClient

class RowDAOImpl(store: NameAndSchemaStore, dc: DataCoordinatorClient, qc: QueryCoordinatorClient) extends RowDAO {
  def query(resourceName: ResourceName, query: String): Result = {
    store.translateResourceName(resourceName) match {
      case Some((datasetId, schemaLite)) =>
        val (code, response) = qc.query(datasetId, query, schemaLite)
        Success(code, response) // TODO: Gah I don't even know where to BEGIN listing the things that need doing here!
      case None =>
        NotFound(resourceName)
    }
  }
}
