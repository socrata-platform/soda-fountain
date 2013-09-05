package com.socrata.soda.server.resources

import com.socrata.soda.server.id.ResourceName
import com.socrata.soql.environment.ColumnName
import com.socrata.soda.server.highlevel.ColumnDAO
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.socrata.http.server.HttpResponse
import com.socrata.soda.server.{LogTag, SodaUtils}
import javax.servlet.http.HttpServletRequest
import com.socrata.soda.server.wiremodels.{RequestProblem, Extracted, UserProvidedColumnSpec}

case class DatasetColumn(columnDAO: ColumnDAO, maxDatumSize: Int) {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[DatasetColumn])

  def withColumnSpec(request: HttpServletRequest, logTags: LogTag*)(f: UserProvidedColumnSpec => HttpResponse): HttpResponse = {
    UserProvidedColumnSpec.fromRequest(request, maxDatumSize) match {
      case Extracted(datasetSpec) =>
        f(datasetSpec)
      case RequestProblem(err) =>
        SodaUtils.errorResponse(request, err, logTags : _*)
    }
  }

  def response(result: ColumnDAO.Result): HttpResponse = {
    log.info("TODO: Negotiate content type")
    result match {
      case ColumnDAO.Created(spec) => Created ~> SodaUtils.JsonContent(spec)
      case ColumnDAO.Updated(spec) => OK ~> SodaUtils.JsonContent(spec)
      case ColumnDAO.Found(spec) => OK ~> SodaUtils.JsonContent(spec)
      case ColumnDAO.Deleted => NoContent
      case ColumnDAO.NotFound(column) => NotFound /* TODO: content */
      case ColumnDAO.InvalidColumnName(column) => BadRequest /* TODO: content */
    }
  }

  case class service(resourceName: ResourceName, columnName: ColumnName) extends SodaResource {
    override def get = { req =>
      response(columnDAO.getColumn(resourceName, columnName))
    }

    override def delete = { req =>
      response(columnDAO.deleteColumn(resourceName, columnName))
    }

    override def put = { req =>
      withColumnSpec(req, resourceName, columnName) { spec =>
        response(columnDAO.replaceOrCreateColumn(resourceName, columnName, spec))
      }
    }

    override def patch = { req =>
      withColumnSpec(req, resourceName, columnName) { spec =>
        response(columnDAO.updateColumn(resourceName, columnName, spec))
      }
    }
  }
}

