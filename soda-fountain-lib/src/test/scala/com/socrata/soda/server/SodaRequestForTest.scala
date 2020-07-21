package com.socrata.soda.server

import com.socrata.http.server.HttpRequest
import com.socrata.http.client.HttpClient

import com.socrata.soda.clients.datacoordinator.DataCoordinatorClient
import com.socrata.soda.server.highlevel.{ColumnDAO, DatasetDAO, ExportDAO, RowDAO, SnapshotDAO}

class SodaRequestForTest(val httpRequest: HttpRequest) extends SodaRequest {
  override def httpClient: HttpClient = ???
  override def dataCoordinator: DataCoordinatorClient = ???
  override def columnDAO: ColumnDAO = ???
  override def datasetDAO: DatasetDAO = ???
  override def exportDAO: ExportDAO = ???
  override def rowDAO: RowDAO = ???
  override def snapshotDAO: SnapshotDAO = ???
}
