package com.socrata.soda.server

import scala.language.implicitConversions

import com.socrata.http.client.HttpClient
import com.socrata.http.server.{HttpRequest, GeneratedHttpRequestApi}
import com.socrata.http.server.util.RequestId.RequestId

import com.socrata.soda.clients.datacoordinator.DataCoordinatorClient
import com.socrata.soda.server.highlevel.{DatasetDAO, ColumnDAO, RowDAO, ExportDAO, SnapshotDAO}

trait SodaRequest {
  def httpClient: HttpClient
  def dataCoordinator: DataCoordinatorClient

  def httpRequest: HttpRequest
  def datasetDAO: DatasetDAO
  def columnDAO: ColumnDAO
  def rowDAO: RowDAO
  def exportDAO: ExportDAO
  def snapshotDAO: SnapshotDAO

  final def resourceScope = httpRequest.resourceScope
}

object SodaRequest {
  implicit def generatedSodaRequestApi(req: SodaRequest): GeneratedHttpRequestApi =
    req.httpRequest

  implicit def sodaRequestApi(req: SodaRequest): HttpRequest.HttpRequestApi =
    req.httpRequest
}
