package com.socrata.soda.server.highlevel

import java.io.Reader

import com.rojoma.simplearm.v2.ResourceScope
import com.socrata.http.server.HttpResponse
import com.socrata.soda.server.id.{ResourceName, SecondaryId}
import com.socrata.soda.clients.datacoordinator._

import com.socrata.soda.server.persistence.NameAndSchemaStore
import DataCoordinatorClient._


trait ResyncDAO {
  def resync(resourceName: ResourceName, secondary: SecondaryId): ResyncDAO.ResyncResponse
}

object ResyncDAO {
  trait ResyncResponse
  case class Success(secondary: SecondaryId) extends ResyncResponse
  case class DatasetNotInSecondary(secondary: SecondaryId) extends ResyncResponse
  case class DatasetNotFound(resource: ResourceName) extends ResyncResponse
}

case class ResyncDAOImpl(store: NameAndSchemaStore, dc: DataCoordinatorClient) extends ResyncDAO {
  override def resync(resourceName: ResourceName, secondary: SecondaryId): ResyncDAO.ResyncResponse = {
    val dataset = store.translateResourceName(resourceName).map(_.systemId)
    val result = dataset.map(dc.resync(_, secondary))
    result match {
      case Some(ResyncResult(secondary)) => ResyncDAO.Success(secondary)
      case Some(DatasetNotInSecondaryResult(secondary)) => ResyncDAO.DatasetNotInSecondary(secondary)
      case None => ResyncDAO.DatasetNotFound(resourceName)
    }
  }
}
