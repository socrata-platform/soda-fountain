package com.socrata.soda.server.resources

import com.socrata.soda.server.highlevel.ColumnDAO
import com.socrata.soda.server.id.ResourceName
import com.socrata.soql.environment.ColumnName
import javax.servlet.http.HttpServletResponse

// TODO: delete compute endpoint when there are no more requests being sent to it
case class Compute(columnDAO: ColumnDAO) {
  val computeUtils = new ComputeUtils(columnDAO)

  case class service(resourceName: ResourceName, columnName: ColumnName) extends SodaResource {
    override def post = { req => resp =>
      computeUtils.compute(req, resp, req.resourceScope, resourceName, columnName, user(req)) {
        computeUtils.writeComputeResponse(
          resourceName, columnName, HttpServletResponse.SC_OK, _: HttpServletResponse)
      }
    }
  }
}
