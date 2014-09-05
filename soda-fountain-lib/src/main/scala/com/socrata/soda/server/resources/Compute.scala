package com.socrata.soda.server.resources

import com.socrata.soda.server.SodaUtils
import com.socrata.soda.server.computation.ComputedColumnsLike
import com.socrata.soda.server.{errors => SodaError}
import com.socrata.soda.server.highlevel.{RowDAO, ExportDAO}
import com.socrata.soda.server.id.ResourceName
import com.socrata.soda.server.persistence._
import com.socrata.soda.server.util.ETagObfuscator
import com.socrata.soql.environment.ColumnName

case class Compute(store: NameAndSchemaStore,
                      exportDAO: ExportDAO,
                      rowDAO: RowDAO,
                      computedColumns: ComputedColumnsLike,
                      etagObfuscator: ETagObfuscator) {
  val compute = new ComputeUtils(exportDAO, rowDAO, computedColumns)

  case class service(resourceName: ResourceName, columnName: ColumnName) extends SodaResource {
    override def post = {
      req => response =>
        store.translateResourceName(resourceName, None) match {
          case Some(dataset) =>
            computedColumns.findComputedColumns(dataset).find(_.fieldName.name == columnName.name) match {
              case Some(columnToCompute) => compute.compute(req, response, dataset, columnToCompute, user(req))
              case None => SodaUtils.errorResponse(req, SodaError.NotAComputedColumn(columnName))(response)
            }
          case None => SodaUtils.errorResponse(req, SodaError.DatasetNotFound(resourceName))(response)
        }
    }
  }
}
