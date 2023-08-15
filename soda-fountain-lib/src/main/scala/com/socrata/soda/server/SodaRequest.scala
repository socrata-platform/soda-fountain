package com.socrata.soda.server

import scala.language.implicitConversions

import com.socrata.http.client.HttpClient
import com.socrata.http.server.{HttpRequest, GeneratedHttpRequestApi}

import com.socrata.soda.clients.datacoordinator.DataCoordinatorClient
import com.socrata.soda.clients.querycoordinator.QueryCoordinatorClient
import com.socrata.soda.server.highlevel.{DatasetDAO, ColumnDAO, RowDAO, ExportDAO, ResyncDAO}
import com.socrata.soda.server.persistence.NameAndSchemaStore

trait SodaRequest {
  def httpClient: HttpClient
  def dataCoordinator: DataCoordinatorClient
  def queryCoordinator: QueryCoordinatorClient

  def httpRequest: HttpRequest
  def datasetDAO: DatasetDAO
  def columnDAO: ColumnDAO
  def rowDAO: RowDAO
  def exportDAO: ExportDAO
  def resyncDAO: ResyncDAO
  def nameAndSchemaStore: NameAndSchemaStore

  final def resourceScope = httpRequest.resourceScope
}

object SodaRequest {
  implicit def generatedSodaRequestApi(req: SodaRequest): GeneratedHttpRequestApi =
    req.httpRequest

  implicit def sodaRequestApi(req: SodaRequest): HttpRequest.HttpRequestApi =
    req.httpRequest
}
