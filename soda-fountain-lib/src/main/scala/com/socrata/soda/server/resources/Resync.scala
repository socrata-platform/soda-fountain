package com.socrata.soda.server.resources

import com.socrata.http.server.responses.OK
import com.socrata.soda.server.SodaUtils.response
import com.socrata.soda.server.highlevel.ResyncDAO
import com.socrata.soda.server.id.{SecondaryId, ResourceName}
import com.socrata.soda.server.SodaUtils
import com.socrata.soda.server.responses.SecondaryNotFound



case object Resync {
  val log = org.slf4j.LoggerFactory.getLogger(getClass)
  case class service(resource: ResourceName, secondary: SecondaryId) extends SodaResource {
    override def get = req => {
      req.resyncDAO.resync(resource, secondary) match {
        case ResyncDAO.Success =>                      OK
        case ResyncDAO.SecondaryNotFound(secondary) => response(req, SecondaryNotFound(secondary.underlying))
      }
    }
  }
}
