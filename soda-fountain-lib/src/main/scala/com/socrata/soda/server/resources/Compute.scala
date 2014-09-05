package com.socrata.soda.server.resources

import com.socrata.soda.server.computation.ComputedColumnsLike
import com.socrata.soda.server.highlevel.{ColumnDAO, RowDAO, ExportDAO}
import com.socrata.soda.server.id.ResourceName
import com.socrata.soda.server.util.ETagObfuscator
import com.socrata.soql.environment.ColumnName

case class Compute(columnDAO: ColumnDAO,
                      exportDAO: ExportDAO,
                      rowDAO: RowDAO,
                      computedColumns: ComputedColumnsLike,
                      etagObfuscator: ETagObfuscator) {
  val computeUtils = new ComputeUtils(columnDAO, exportDAO, rowDAO, computedColumns)

  case class service(resourceName: ResourceName, columnName: ColumnName) extends SodaResource {
    override def post = { req => response =>
      computeUtils.compute(req, response, resourceName, columnName, user(req))
    }
  }
}
