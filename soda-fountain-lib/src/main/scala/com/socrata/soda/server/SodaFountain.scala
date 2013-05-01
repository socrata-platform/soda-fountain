package com.socrata.soda.server

import com.socrata.soda.server.persistence._
import com.socrata.datacoordinator.client.DataCoordinatorClient
import com.socrata.soda.server.services.{SodaService, CatalogService, ColumnService, DatasetService}

object SodaFountain {
  def getConfiguredStore: NameAndSchemaStore = new PostgresNameAndSchemaStore()
  def getConfiguredClient: DataCoordinatorClient = new DataCoordinatorClient("http://localhost:12345")

}

class SodaFountain(val store: NameAndSchemaStore, val dc: DataCoordinatorClient) extends SodaService {

  val data = new SodaFountain(store, dc) with DatasetService
  val columns = new SodaFountain(store, dc) with ColumnService
  val catalog = new SodaFountain(store, dc) with CatalogService

  def router = new SodaRouter(data, columns, catalog)
}