package com.socrata.soda.server.resources

import com.socrata.http.server.responses.{Json, OK}
import com.socrata.http.server.implicits._
import com.socrata.soda.server.SodaUtils.response
import com.socrata.soda.server.highlevel.ResyncDAO
import com.socrata.soda.server.id.{SecondaryId, ResourceName}
import com.socrata.soda.server.SodaUtils
import com.socrata.soda.server.responses.{DatasetNotInSecondary, DatasetNotFound}



case object Resync {
  val log = org.slf4j.LoggerFactory.getLogger(getClass)
  case class service(resource: ResourceName, secondary: SecondaryId) extends SodaResource {
    override def put = req => {
      req.resyncDAO.resync(resource, secondary) match {
        case ResyncDAO.Success(_) =>                      OK
        case ResyncDAO.DatasetNotInSecondary(secondary, dataset) => response(req, DatasetNotInSecondary(secondary.underlying, dataset))
        case ResyncDAO.DatasetNotFound(resource) => response(req, DatasetNotFound(resource))
      }
    }
  }
}
