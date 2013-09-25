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
import com.socrata.soda.server.errors.{ResourceNotModified, EtagPreconditionFailed}

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

  def response(req: HttpServletRequest, result: ColumnDAO.Result, etagSuffix: String, isGet: Boolean = false): HttpResponse = {
    log.info("TODO: Negotiate content type")
    result match {
      case ColumnDAO.Created(spec, etagOpt) =>
        etagOpt.foldLeft(Created) { (root, etag) => root ~> Header("ETag", etagObfuscator.obfuscate(etag.map(_ + etagSuffix)).toString) } ~> SodaUtils.JsonContent(spec)
      case ColumnDAO.Updated(spec, etag) => OK ~> SodaUtils.JsonContent(spec)
      case ColumnDAO.Found(spec, etag) => OK ~> SodaUtils.JsonContent(spec)
      case ColumnDAO.Deleted => NoContent
      case ColumnDAO.ColumnNotFound(column) => NotFound /* TODO: content */
      case ColumnDAO.DatasetNotFound(dataset) => NotFound /* TODO: content */
      case ColumnDAO.InvalidColumnName(column) => BadRequest /* TODO: content */
      case ColumnDAO.PreconditionFailed(Precondition.FailedBecauseMatch(etags)) =>
        if(isGet) {
          log.info("TODO: when we have content-negotiation, set the Vary parameter on ResourceNotModified")
          SodaUtils.errorResponse(req, ResourceNotModified(etags.map(_.map(_ + etagSuffix)).map(etagObfuscator.obfuscate), None))
        } else {
          SodaUtils.errorResponse(req, EtagPreconditionFailed)
        }
      case ColumnDAO.PreconditionFailed(Precondition.FailedBecauseNoMatch) =>
        SodaUtils.errorResponse(req, EtagPreconditionFailed)
    }
  }

  def checkPrecondition(req: HttpServletRequest, isGet: Boolean = false)(op: Precondition => ColumnDAO.Result): HttpResponse = {
    val suffix = "+"
    req.precondition.map(etagObfuscator.deobfuscate).filter(_.value.endsWith(suffix)) match {
      case Right(preconditionRaw) =>
        val precondition = preconditionRaw.map(_.map(_.dropRight(suffix.length)))
        response(req, op(precondition), suffix, isGet)
      case Left(Precondition.FailedBecauseNoMatch) =>
        SodaUtils.errorResponse(req, EtagPreconditionFailed)
      case Left(Precondition.FailedBecauseMatch(etags)) =>
        log.info("TODO: when we have content-negotiation, set the Vary parameter on ResourceNotModified")
        if(isGet) SodaUtils.errorResponse(req, ResourceNotModified(etags.map(etagObfuscator.obfuscate), None))
        else SodaUtils.errorResponse(req, EtagPreconditionFailed)
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
        columnDAO.deleteColumn(resourceName, columnName)
      }
    }

    override def put = { req =>
      withColumnSpec(req, resourceName, columnName) { spec =>
        checkPrecondition(req) { precondition =>
          columnDAO.replaceOrCreateColumn(resourceName, precondition, columnName, spec)
        }
      }
    }

    override def patch = { req =>
      withColumnSpec(req, resourceName, columnName) { spec =>
        checkPrecondition(req) { precondition =>
          columnDAO.updateColumn(resourceName, columnName, spec)
        }
      }
    }
  }
}

