package com.socrata.soda.server.resources

import com.rojoma.json.v3.ast.{JObject, JString}
import com.rojoma.json.v3.interpolation._
import com.rojoma.simplearm.v2.ResourceScope
import com.socrata.http.server.{HttpRequest, HttpResponse}
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.util.{EntityTag, Precondition, RequestId}
import com.socrata.soda.server._
import com.socrata.soda.server.responses._
import com.socrata.soda.server.highlevel._
import com.socrata.soda.server.id.ResourceName
import com.socrata.soda.server.util.ETagObfuscator
import com.socrata.soda.server.wiremodels.InputUtils.jsonSingleObjectStream
import com.socrata.soda.server.wiremodels._
import com.socrata.soql.environment.ColumnName
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

case class DatasetColumn(etagObfuscator: ETagObfuscator, maxDatumSize: Int) {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[DatasetColumn])
  val defaultSuffix = Array[Byte]('+')

  def withColumnSpec(request: HttpRequest, response: HttpServletResponse, logTags: LogTag*)(f: UserProvidedColumnSpec => Unit): Unit = {
    UserProvidedColumnSpec.fromRequest(request, maxDatumSize) match {
      case Extracted(datasetSpec) =>
        f(datasetSpec)
      case RequestProblem(err) =>
        SodaUtils.response(request, err, logTags : _*)(response)
      case IOProblem(err) =>
        SodaUtils.internalError(request, err)(response)
    }
  }

  def response(req: SodaRequest, result: ColumnDAO.Result, etagSuffix: Array[Byte] = defaultSuffix, isGet: Boolean = false): HttpResponse = {
    // TODO: Negotiate content type
    def prepareETag(etag: EntityTag) = etagObfuscator.obfuscate(etag.append(etagSuffix))
    result match { // ugh why doesn't this use SodaUtils.Response?
      case ColumnDAO.Created(column, etagOpt) =>
        etagOpt.foldLeft(Created: HttpResponse) { (root, etag) => root ~> ETag(prepareETag(etag)) } ~> Json(column.asSpec)
      case ColumnDAO.Updated(column, etag) =>
        OK ~> Json(column.asSpec)
      case ColumnDAO.Found(ds, column, etag) =>
        OK ~> Json(column.asSpec)
      case ColumnDAO.Deleted(column, etag) =>
        OK ~> Json(column.asSpec)
      case ColumnDAO.ColumnAlreadyExists(columnName) =>
        Conflict ~> Json(ColumnSpecSubSet(None, Some(columnName)))
      case ColumnDAO.DatasetVersionMismatch(dataset, version) =>
        Conflict ~> Json(json"""{version: $version}""")
      case ColumnDAO.IllegalColumnId(columnName) =>
        BadRequest ~> Json(ColumnSpecSubSet(None, Some(columnName)))
      case ColumnDAO.ColumnNotFound(columnName) =>
        NotFound ~> Json(ColumnSpecSubSet(None, Some(columnName)))
      case ColumnDAO.DatasetNotFound(dataset) =>
        NotFound ~> Json(DatasetSpecSubSet(dataset))
      case ColumnDAO.InvalidColumnName(columnName) =>
        BadRequest ~> Json(ColumnSpecSubSet(None, Some(columnName)))
      case ColumnDAO.InvalidSystemColumnOperation(columnName) =>
        BadRequest ~> Json(ColumnSpecSubSet(None, Some(columnName)))
      case ColumnDAO.ColumnHasDependencies(col, deps) =>
        SodaUtils.response(req, ColumnHasDependencies(col, deps))
      case ColumnDAO.CannotDeleteRowId(column, method) =>
        SodaUtils.response(req, HttpMethodNotAllowed(method, Seq("GET", "PATCH")))
      case ColumnDAO.DuplicateValuesInColumn(column) =>
        SodaUtils.response(req, NonUniqueRowId(column.fieldName))
      case ColumnDAO.ComputationStrategyValidationError(error) =>
        BadRequest ~> Json(error.message)
      case ColumnDAO.ComputationStrategyValidationErrorResult(result) =>
        BadRequest ~> Json(result.getClass.getSimpleName)
      case ColumnDAO.ColumnValidationError(result) =>
        BadRequest ~> Json(result.getClass.getSimpleName)
      case ColumnDAO.InternalServerError(code, tag, data) =>
        SodaUtils.response(req, InternalError(tag,
          "code"  -> JString(code),
          "data" -> JString(data)
        ))
      case ColumnDAO.PreconditionFailed(Precondition.FailedBecauseMatch(etags)) =>
        if(isGet) {
          // TODO: when we have content-negotiation, set the Vary parameter on ResourceNotModified
          SodaUtils.response(req, ResourceNotModified(etags.map(prepareETag), None))
        } else {
          SodaUtils.response(req, EtagPreconditionFailed)
        }
      case ColumnDAO.PreconditionFailed(Precondition.FailedBecauseNoMatch) =>
        SodaUtils.response(req, EtagPreconditionFailed)
      case ColumnDAO.EmptyResult =>
        NoContent
    }
  }

  def checkPrecondition(req: SodaRequest, suffix: Array[Byte] = defaultSuffix, isGet: Boolean = false)
                       (op: Precondition => Unit): Unit = {
    req.precondition.map(etagObfuscator.deobfuscate).filter(_.endsWith(suffix)) match {
      case Right(preconditionRaw) =>
        op(preconditionRaw.map(_.dropRight(suffix.length)))
      case Left(Precondition.FailedBecauseNoMatch) =>
        SodaUtils.response(req, EtagPreconditionFailed)
    }
  }

  case class service(resourceName: ResourceName, columnName: ColumnName) extends SodaResource {
    override def get = { req => resp =>
      checkPrecondition(req, isGet = true) { precondition =>
        response(req, req.columnDAO.getColumn(resourceName, columnName))(resp)
      }
    }

    override def delete = { req => resp =>
      checkPrecondition(req) { precondition =>
        response(req, req.columnDAO.deleteColumn(user(req), resourceName, expectedDataVersion(req), columnName,
                                                 req.requestId))(resp)
      }
    }

    override def put = { req => resp =>
      val userFromReq = user(req)
      val edvFromReq = expectedDataVersion(req)
      val requestId = req.requestId
      withColumnSpec(req.httpRequest, resp, resourceName, columnName) { spec =>
        checkPrecondition(req) { precondition =>
          response(req, req.columnDAO.replaceOrCreateColumn(userFromReq, resourceName, precondition, edvFromReq, columnName, spec, requestId))(resp)
        }
      }
    }

    override def patch = { req => resp =>
      withColumnSpec(req.httpRequest, resp, resourceName, columnName) { spec =>
        checkPrecondition(req) { precondition =>
          response(req, req.columnDAO.updateColumn(user(req), resourceName, expectedDataVersion(req), columnName, spec, req.requestId))(resp)
        }
      }
    }
  }

  case class pkservice(resourceName: ResourceName, columnName: ColumnName) extends SodaResource {
    override def post = { req => resp =>
      response(req,
               req.columnDAO.makePK(user(req), resourceName, expectedDataVersion(req), columnName),
               Array[Byte](0))(resp)
    }
  }

  case class indexDirectiveService(resourceName: ResourceName, columnName: ColumnName) extends SodaResource {
    override def post = { req => resp =>
      checkPrecondition(req) { precondition =>
        jsonSingleObjectStream(req.httpRequest, 10000) match {
          case Right(directive) =>
            response(req,
              req.columnDAO.createOrUpdateIndexDirective(user(req), resourceName, expectedDataVersion(req), columnName,
                directive, req.requestId), Array[Byte](0))(resp)
          case Left(err) =>
            RequestProblem(err)
        }
      }
    }

    override def delete = { req => resp =>
      checkPrecondition(req) { precondition =>
        response(req, req.columnDAO.dropIndexDirectives(user(req), resourceName, expectedDataVersion(req), columnName,
          req.requestId))(resp)
      }
    }
  }
}

