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
import com.socrata.soda.server.util.ETagObfuscator
import com.socrata.http.server.util.Precondition

case class DatasetColumn(columnDAO: ColumnDAO, etagObfuscator: ETagObfuscator, maxDatumSize: Int) {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[DatasetColumn])

  def withColumnSpec(request: HttpServletRequest, logTags: LogTag*)(f: UserProvidedColumnSpec => HttpResponse): HttpResponse = {
    UserProvidedColumnSpec.fromRequest(request, maxDatumSize) match {
      case Extracted(datasetSpec) =>
        f(datasetSpec)
      case RequestProblem(err) =>
        SodaUtils.errorResponse(request, err, logTags : _*)
    }
  }

  def response(result: ColumnDAO.Result, isGet: Boolean = false): HttpResponse = {
    log.info("TODO: Negotiate content type")
    result match {
      case ColumnDAO.Created(spec, etagOpt) =>
        etagOpt.foldLeft(Created) { (root, etag) => root ~> Header("ETag", etagObfuscator.obfuscate(etag).toString) } ~> SodaUtils.JsonContent(spec)
      case ColumnDAO.Updated(spec, etag) => OK ~> SodaUtils.JsonContent(spec)
      case ColumnDAO.Found(spec, etag) => OK ~> SodaUtils.JsonContent(spec)
      case ColumnDAO.Deleted => NoContent
      case ColumnDAO.ColumnNotFound(column) => NotFound /* TODO: content */
      case ColumnDAO.DatasetNotFound(dataset) => NotFound /* TODO: content */
      case ColumnDAO.InvalidColumnName(column) => BadRequest /* TODO: content */
      case ColumnDAO.PreconditionFailed(Precondition.FailedBecauseMatch(etags)) =>
        if(isGet) {
          etags.foldLeft(NotModified) { (root, etag) => root ~> Header("ETag", etagObfuscator.obfuscate(etag).toString) } // no content is fine here
        } else {
          etags.foldLeft(PreconditionFailed) { (root, etag) => root ~> Header("ETag", etagObfuscator.obfuscate(etag).toString) } /* TODO: content */
        }
      case ColumnDAO.PreconditionFailed(Precondition.FailedBecauseNoMatch) =>
        PreconditionFailed /* TODO: content */
    }
  }

  case class service(resourceName: ResourceName, columnName: ColumnName) extends SodaResource {
    override def get = { req =>
      response(columnDAO.getColumn(resourceName, columnName), isGet = true)
    }

    override def delete = { req =>
      response(columnDAO.deleteColumn(resourceName, columnName))
    }

    override def put = { req =>
      withColumnSpec(req, resourceName, columnName) { spec =>
        response(columnDAO.replaceOrCreateColumn(resourceName, req.precondition.map(etagObfuscator.deobfuscate), columnName, spec))
      }
    }

    override def patch = { req =>
      withColumnSpec(req, resourceName, columnName) { spec =>
        response(columnDAO.updateColumn(resourceName, columnName, spec))
      }
    }
  }
}

