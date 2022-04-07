package com.socrata.soda.server.highlevel

import java.io.Reader

import com.rojoma.simplearm.v2.ResourceScope
import com.socrata.http.server.HttpResponse
import com.socrata.soda.server.id.{ResourceName, SecondaryId}

import com.socrata.soda.server.persistence.NameAndSchemaStore


trait ResyncDAO {
  def resync(resourceName: ResourceName, secondary: SecondaryId): ResyncDAO.ResyncResponse
}

object ResyncDAO {
  trait ResyncResponse
  case object Success extends ResyncResponse
  case class SecondaryNotFound(secondary: SecondaryId) extends ResyncResponse
}

case class ResyncDAOImpl(store: NameAndSchemaStore) extends ResyncDAO {
  override def resync(resourceName: ResourceName, secondary: SecondaryId): ResyncDAO.ResyncResponse = ResyncDAO.Success
  // do sql here
}
