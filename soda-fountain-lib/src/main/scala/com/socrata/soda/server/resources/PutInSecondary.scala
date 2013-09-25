package com.socrata.soda.server.resources

import com.socrata.soda.server.id.{SecondaryId, ResourceName}
import com.socrata.soda.server.highlevel.DatasetDAO
import com.socrata.http.server.responses._

case class PutInSecondary(datasetDAO: DatasetDAO) {
  val es = SecondaryId("es")

  case class service(resourceName: ResourceName) extends SodaResource {
    override def post = { req =>
      datasetDAO.propagateToSecondary(resourceName, es) match {
        case DatasetDAO.Updated(datasetSpec) =>
          OK
        case DatasetDAO.NotFound(name) =>
          NotFound
      }
    }
  }
}
