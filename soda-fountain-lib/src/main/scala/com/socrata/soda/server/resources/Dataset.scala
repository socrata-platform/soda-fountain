package com.socrata.soda.server.resources

import com.socrata.http.server.{HttpRequest, HttpResponse}
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.util.RequestId
import com.socrata.soda.server._
import com.socrata.soda.server.copy.Stage
import com.socrata.soda.server.errors.{GeneralNotFoundError, RollupNotFound, RollupColumnNotFound, RollupCreationFailed}
import com.socrata.soda.server.highlevel._
import com.socrata.soda.server.highlevel.DatasetDAO
import com.socrata.soda.server.id.{RollupName, SecondaryId, ResourceName}
import com.socrata.soda.server.wiremodels.{UserProvidedSpec, Extracted, UserProvidedDatasetSpec, UserProvidedRollupSpec}
import com.socrata.soda.server.wiremodels.{RequestProblem, IOProblem}
import com.socrata.soda.server.{SodaUtils, LogTag}
import javax.servlet.http.HttpServletRequest

/**
 * Dataset: CRUD operations for dataset schema and metadata
 */
case class Dataset(datasetDAO: DatasetDAO, maxDatumSize: Int) {

  val schemaHashHeaderName = "x-socrata-version-hash"
  val log = org.slf4j.LoggerFactory.getLogger(classOf[Dataset])

  def withDatasetSpec(request: HttpRequest, logTags: LogTag*)(f: UserProvidedDatasetSpec => HttpResponse): HttpResponse = {
    UserProvidedDatasetSpec.fromRequest(request, maxDatumSize) match {
      case Extracted(datasetSpec) =>
        f(datasetSpec)
      case RequestProblem(err) =>
        SodaUtils.errorResponse(request, err, logTags : _*)
      case IOProblem(err) =>
        SodaUtils.internalError(request, err)
    }
  }

  def withRollupSpec(request: HttpRequest, logTags: LogTag*)(f: UserProvidedRollupSpec => HttpResponse): HttpResponse = {
    UserProvidedRollupSpec.fromRequest(request, maxDatumSize) match {
      case Extracted(datasetSpec) =>
        f(datasetSpec)
      case RequestProblem(err) =>
        SodaUtils.errorResponse(request, err, logTags : _*)
      case IOProblem(err) =>
        SodaUtils.internalError(request, err)
    }
  }

  def response(req: HttpRequest, result: DatasetDAO.Result): HttpResponse = {
    // TODO: Negotiate content type
    log.info(s"sending response, result: ${result}")
    result match {
      case DatasetDAO.Created(record) => Created ~> Json(record.asSpec)
      case DatasetDAO.Updated(record) => OK ~> Json(record.asSpec)
      case DatasetDAO.Found(record) => OK ~> Json(record.asSpec)
      case DatasetDAO.DatasetVersion(vr) => OK ~> Json(vr)
      case DatasetDAO.Deleted => NoContent
      case DatasetDAO.NotFound(dataset) => NotFound /* TODO: content */
      case DatasetDAO.InvalidDatasetName(name) => BadRequest /* TODO: content */
      case DatasetDAO.InvalidColumnName(name) => BadRequest /* TODO: content */
      case DatasetDAO.WorkingCopyCreated => Created /* TODO: content */
      case DatasetDAO.PropagatedToSecondary => Created /* TODO: content */
      case DatasetDAO.WorkingCopyDropped => NoContent
      case DatasetDAO.WorkingCopyPublished => NoContent
      case DatasetDAO.RollupCreatedOrUpdated => NoContent
      case DatasetDAO.RollupNotFound(name) => SodaUtils.errorResponse(req, RollupNotFound(name))
      case DatasetDAO.RollupDropped => NoContent
      case DatasetDAO.RollupError(reason) =>  SodaUtils.errorResponse(req, RollupCreationFailed(reason))
      case DatasetDAO.RollupColumnNotFound(column) => SodaUtils.errorResponse(req, RollupColumnNotFound(column))
      // TODO other cases have not been implemented
      case _@x =>
        log.warn("case is NOT implemented")
        ???
    }
  }

  object createService extends SodaResource {
    override def post = { req =>
      withDatasetSpec(req) { spec =>
        response(req, datasetDAO.createDataset(user(req), spec, RequestId.getFromRequest(req)))
      }
    }
  }

  case class service(resourceName: ResourceName) extends SodaResource {
    override def put = { req =>
      withDatasetSpec(req, resourceName) { spec =>
        response(req, datasetDAO.replaceOrCreateDataset(user(req), resourceName, spec,
                                                        RequestId.getFromRequest(req)))
      }
    }

    override def get = { req =>
      val copy = Stage(req.getParameter("$$copy")) // Query parameter for copy.  Optional, "latest", "published", "unpublished", or "latest"
      response(req, datasetDAO.getDataset(resourceName, copy))
    }

    override def patch = { req =>
      withDatasetSpec(req, resourceName) { spec =>
        response(req, datasetDAO.updateDataset(user(req), resourceName, spec,
                                               RequestId.getFromRequest(req)))
      }
    }

    override def delete = { req =>
      response(req, datasetDAO.deleteDataset(user(req), resourceName, RequestId.getFromRequest(req)))
    }
  }

  case class copyService(resourceName: ResourceName) extends SodaResource {
    def snapshotLimit(req: HttpServletRequest) =
      try { Option(req.getParameter("snapshot_limit")).map(_.toInt) }
      catch { case e: NumberFormatException => ??? /* TODO: Proper error */ }

    // TODO: not GET
    override def get = { req =>
      val doCopyData = req.getParameter("copy_data") == "true"
      response(req, datasetDAO.makeCopy(user(req), resourceName, copyData = doCopyData,
                                        requestId = RequestId.getFromRequest(req)))
    }

    override def delete = { req =>
      response(req, datasetDAO.dropCurrentWorkingCopy(user(req), resourceName,
                                                      RequestId.getFromRequest(req)))
    }
    override def put = { req =>
      response(req, datasetDAO.publish(user(req), resourceName, snapshotLimit = snapshotLimit(req),
                                       requestId = RequestId.getFromRequest(req)))
    }
  }

  case class versionService(resourceName: ResourceName, secondary: SecondaryId) extends SodaResource {
    override def get = { req =>
      response(req, datasetDAO.getVersion(resourceName, secondary, RequestId.getFromRequest(req)))
    }
  }

  case class secondaryCopyService(resourceName: ResourceName, secondary: SecondaryId) extends SodaResource {
    override def post = { req =>
      response(req, datasetDAO.propagateToSecondary(resourceName, secondary, RequestId.getFromRequest(req)))
    }
  }

  case class rollupService(resourceName: ResourceName, rollupName: RollupName) extends SodaResource {
    override def get = { req =>
      // TODO Not implemented yet
//      response(req, datasetDAO.getRollup(user(req), resourceName, rollupName))
      MethodNotAllowed
    }

    override def delete = { req =>
      response(req, datasetDAO.deleteRollup(user(req), resourceName, rollupName,
                                            RequestId.getFromRequest(req)))
    }

    override def put = { req =>
      withRollupSpec(req) { spec =>
        response(req, datasetDAO.replaceOrCreateRollup(user(req), resourceName, rollupName, spec,
                                                       RequestId.getFromRequest(req)))
      }
    }
  }
}
