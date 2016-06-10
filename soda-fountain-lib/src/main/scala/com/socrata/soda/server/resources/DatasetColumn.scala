package com.socrata.soda.server.resources

import com.rojoma.json.v3.ast.JString
import com.socrata.http.server.{HttpRequest, HttpResponse}
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.util.{EntityTag, Precondition, RequestId}
import com.socrata.soda.server._
import com.socrata.soda.server.computation.ComputedColumnsLike
import com.socrata.soda.server.responses._
import com.socrata.soda.server.highlevel._
import com.socrata.soda.server.id.ResourceName
import com.socrata.soda.server.util.ETagObfuscator
import com.socrata.soda.server.wiremodels._
import com.socrata.soql.environment.ColumnName
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}

case class DatasetColumn(columnDAO: ColumnDAO, exportDAO: ExportDAO, rowDAO: RowDAO, computedColumns: ComputedColumnsLike, etagObfuscator: ETagObfuscator, maxDatumSize: Int) {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[DatasetColumn])
  val computeUtils = new ComputeUtils(columnDAO, exportDAO, rowDAO, computedColumns)
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

  def response(req: HttpServletRequest, result: ColumnDAO.Result, etagSuffix: Array[Byte] = defaultSuffix, isGet: Boolean = false): HttpResponse = {
    // TODO: Negotiate content type
    def prepareETag(etag: EntityTag) = etagObfuscator.obfuscate(etag.append(etagSuffix))
    result match {
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
    }
  }

  def checkPrecondition(req: HttpRequest, suffix: Array[Byte] = defaultSuffix, isGet: Boolean = false)
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
        response(req, columnDAO.getColumn(resourceName, columnName))(resp)
      }
    }

    override def delete = { req => resp =>
      checkPrecondition(req) { precondition =>
        response(req, columnDAO.deleteColumn(user(req), resourceName, columnName,
                                             RequestId.getFromRequest(req)))(resp)
      }
    }

    override def put = { req => resp =>
      val userFromReq = user(req)
      val requestId = RequestId.getFromRequest(req)
      withColumnSpec(req, resp, resourceName, columnName) { spec =>
        checkPrecondition(req) { precondition =>
          columnDAO.replaceOrCreateColumn(userFromReq, resourceName, precondition, columnName,
                                          spec, requestId) match {
            case success: ColumnDAO.UpdateSuccessResult =>
              if (spec.computationStrategy.isDefined &&
                  // optionally break apart rows computation into separate call.
                  "false" != req.getParameter("compute")) {
                try {
                  computeUtils.compute(req, resp, resourceName, columnName, user(req)) {
                    case (res, report) => response(req, success)(resp)
                  }
                } catch { case e: Exception =>
                  columnDAO.deleteColumn(userFromReq, resourceName, columnName, requestId)
                  throw e
                }
              }
              else response(req, success)(resp)
            case other => response(req, other)(resp)
          }
        }
      }
    }

    override def patch = { req => resp =>
      withColumnSpec(req, resp, resourceName, columnName) { spec =>
        checkPrecondition(req) { precondition =>
          response(req, columnDAO.updateColumn(user(req), resourceName, columnName, spec, RequestId.getFromRequest(req)))(resp)
        }
      }
    }
  }

  case class pkservice(resourceName: ResourceName, columnName: ColumnName) extends SodaResource {
    override def post = { req => resp =>
      response(req,
               columnDAO.makePK(user(req), resourceName, columnName,
                                RequestId.getFromRequest(req)),
               Array[Byte](0))(resp)
    }
  }
}

