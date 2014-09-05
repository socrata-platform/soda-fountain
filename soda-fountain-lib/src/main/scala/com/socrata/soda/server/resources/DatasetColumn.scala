package com.socrata.soda.server.resources

import com.socrata.http.server.HttpResponse
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.util.{EntityTag, Precondition}
import com.socrata.soda.server.errors.{NonUniqueRowId, HttpMethodNotAllowed, ResourceNotModified, EtagPreconditionFailed}
import com.socrata.soda.server.highlevel._
import com.socrata.soda.server.id.ResourceName
import com.socrata.soda.server.util.ETagObfuscator
import com.socrata.soda.server.wiremodels.{RequestProblem, Extracted, UserProvidedColumnSpec}
import com.socrata.soda.server.{LogTag, SodaUtils}
import com.socrata.soql.environment.ColumnName
import javax.servlet.http.HttpServletRequest

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

  def response(req: HttpServletRequest, result: ColumnDAO.Result, etagSuffix: Array[Byte], isGet: Boolean = false): HttpResponse = {
    log.info("TODO: Negotiate content type")
    def prepareETag(etag: EntityTag) = etagObfuscator.obfuscate(etag.append(etagSuffix))
    result match {
      case ColumnDAO.Created(column, etagOpt) =>
        etagOpt.foldLeft(Created) { (root, etag) => root ~> ETag(prepareETag(etag)) } ~> SodaUtils.JsonContent(column.asSpec)
      case ColumnDAO.Updated(column, etag) => OK ~> SodaUtils.JsonContent(column.asSpec)
      case ColumnDAO.Found(ds, column, etag) => OK ~> SodaUtils.JsonContent(column.asSpec)
      case ColumnDAO.Deleted(column, etag) => OK ~> SodaUtils.JsonContent(column.asSpec)
      case ColumnDAO.ColumnNotFound(column) => NotFound /* TODO: content */
      case ColumnDAO.DatasetNotFound(dataset) => NotFound /* TODO: content */
      case ColumnDAO.InvalidColumnName(column) => BadRequest /* TODO: content */
      case ColumnDAO.InvalidRowIdOperation(column, method) =>
        SodaUtils.errorResponse(req, HttpMethodNotAllowed(method, Seq("GET", "PATCH")))
      case ColumnDAO.NonUniqueRowId(column) =>
        SodaUtils.errorResponse(req, NonUniqueRowId(column.name))
      case ColumnDAO.PreconditionFailed(Precondition.FailedBecauseMatch(etags)) =>
        if(isGet) {
          log.info("TODO: when we have content-negotiation, set the Vary parameter on ResourceNotModified")
          SodaUtils.errorResponse(req, ResourceNotModified(etags.map(prepareETag), None))
        } else {
          SodaUtils.errorResponse(req, EtagPreconditionFailed)
        }
      case ColumnDAO.PreconditionFailed(Precondition.FailedBecauseNoMatch) =>
        SodaUtils.errorResponse(req, EtagPreconditionFailed)
    }
  }

  def checkPrecondition(req: HttpServletRequest, isGet: Boolean = false)(op: Precondition => ColumnDAO.Result): HttpResponse = {
    val suffix = Array[Byte]('+')
    req.precondition.map(etagObfuscator.deobfuscate).filter(_.endsWith(suffix)) match {
      case Right(preconditionRaw) =>
        val precondition = preconditionRaw.map(_.dropRight(suffix.length))
        response(req, op(precondition), suffix, isGet)
      case Left(Precondition.FailedBecauseNoMatch) =>
        SodaUtils.errorResponse(req, EtagPreconditionFailed)
    }
  }

  case class service(resourceName: ResourceName, columnName: ColumnName) extends SodaResource {
    override def get = { req =>
      checkPrecondition(req, isGet = true) { precondition =>
        columnDAO.getColumn(resourceName, columnName)
      }
    }

    override def delete = { req =>
      checkPrecondition(req) { precondition =>
        columnDAO.deleteColumn(user(req), resourceName, columnName)
      }
    }

    override def put = { req =>
      withColumnSpec(req, resourceName, columnName) { spec =>
        checkPrecondition(req) { precondition =>
          columnDAO.replaceOrCreateColumn(user(req), resourceName, precondition, columnName, spec)
        }
      }
    }

    override def patch = { req =>
      withColumnSpec(req, resourceName, columnName) { spec =>
        checkPrecondition(req) { precondition =>
          columnDAO.updateColumn(user(req), resourceName, columnName, spec)
        }
      }
    }
  }

  case class pkservice(resourceName: ResourceName, columnName: ColumnName) extends SodaResource {
    override def post = { req =>
      response(req, columnDAO.makePK(user(req), resourceName, columnName), new Array[Byte](0))
    }
  }
}

