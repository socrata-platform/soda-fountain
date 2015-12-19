package com.socrata.soda.server.resources

import com.socrata.soda.clients.datacoordinator.DataCoordinatorClient.ReportItem
import com.socrata.soda.server.computation.ComputedColumnsLike
import com.socrata.soda.server.highlevel.{ColumnDAO, RowDAO, ExportDAO}
import com.socrata.soda.server.id.ResourceName
import com.socrata.soda.server.metrics.MetricProvider
import com.socrata.soda.server.metrics.Metrics.Metric
import com.socrata.soda.server.util.ETagObfuscator
import com.socrata.soql.environment.ColumnName
import javax.servlet.http.HttpServletResponse

case class Compute(columnDAO: ColumnDAO,
                      exportDAO: ExportDAO,
                      rowDAO: RowDAO,
                      computedColumns: ((Metric => Unit) => ComputedColumnsLike),
                      metricProvider: MetricProvider,
                      etagObfuscator: ETagObfuscator) {
  val computeUtils = new ComputeUtils(columnDAO, exportDAO, rowDAO, computedColumns, metricProvider)

  case class service(resourceName: ResourceName, columnName: ColumnName) extends SodaResource {
    override def post = { req => resp =>
      computeUtils.compute(req, resp, resourceName, columnName, user(req)) {
        computeUtils.writeComputeResponse(
          resourceName, columnName, HttpServletResponse.SC_OK, _: HttpServletResponse, _: Iterator[ReportItem])
      }
    }
  }
}
