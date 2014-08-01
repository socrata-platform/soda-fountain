package com.socrata.soda.server.resources

import com.socrata.http.server.HttpResponse
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.soda.server.errors.{RollupNotFound, RollupColumnNotFound, RollupCreationFailed}
import com.socrata.soda.server.highlevel.DatasetDAO
import com.socrata.soda.server.id.{RollupName, SecondaryId, ResourceName}
import com.socrata.soda.server.wiremodels.{UserProvidedSpec, RequestProblem, Extracted, UserProvidedDatasetSpec, UserProvidedRollupSpec}
import com.socrata.soda.server.{SodaUtils, LogTag}
import javax.servlet.http.HttpServletRequest
import com.socrata.soda.server.copy.Stage

/**
 * Dataset: CRUD operations for dataset schema and metadata
 */
case class Dataset(datasetDAO: DatasetDAO, maxDatumSize: Int) {

  val schemaHashHeaderName = "x-socrata-version-hash"
  val log = org.slf4j.LoggerFactory.getLogger(classOf[Dataset])

  def withDatasetSpec(request: HttpServletRequest, logTags: LogTag*)(f: UserProvidedDatasetSpec => HttpResponse): HttpResponse = {
    UserProvidedDatasetSpec.fromRequest(request, maxDatumSize) match {
      case Extracted(datasetSpec) =>
        f(datasetSpec)
      case RequestProblem(err) =>
        SodaUtils.errorResponse(request, err, logTags : _*)
    }
  }

  def withRollupSpec(request: HttpServletRequest, logTags: LogTag*)(f: UserProvidedRollupSpec => HttpResponse): HttpResponse = {
    UserProvidedRollupSpec.fromRequest(request, maxDatumSize) match {
      case Extracted(datasetSpec) =>
        f(datasetSpec)
      case RequestProblem(err) =>
        SodaUtils.errorResponse(request, err, logTags : _*)
    }
  }

  def response(req: HttpServletRequest, result: DatasetDAO.Result): HttpResponse = {
    // TODO: Negotiate content type
    log.info(s"sending response, result: ${result}")
    result match {
      case DatasetDAO.Created(spec) => Created ~> SodaUtils.JsonContent(spec)
      case DatasetDAO.Updated(spec) => OK ~> SodaUtils.JsonContent(spec)
      case DatasetDAO.Found(spec) => OK ~> SodaUtils.JsonContent(spec)
      case DatasetDAO.DatasetVersion(vr) => OK ~> SodaUtils.JsonContent(vr)
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
    }
  }

  object createService extends SodaResource {
    override def post = { req =>
      withDatasetSpec(req) { spec =>
        response(req, datasetDAO.createDataset(user(req), spec))
      }
    }
  }

  case class service(resourceName: ResourceName) extends SodaResource {
    override def put = { req =>
      withDatasetSpec(req, resourceName) { spec =>
        response(req, datasetDAO.replaceOrCreateDataset(user(req), resourceName, spec))
      }
    }

    override def get = { req =>
      val copy = Stage(req.getParameter("$$copy")) // Query parameter for copy.  Optional, "latest", "published", "unpublished", or "latest"
      response(req, datasetDAO.getDataset(resourceName, copy))
    }

    override def patch = { req =>
      withDatasetSpec(req, resourceName) { spec =>
        response(req, datasetDAO.updateDataset(user(req), resourceName, spec))
      }
    }

    override def delete = { req =>
      response(req, datasetDAO.deleteDataset(user(req), resourceName))
    }
  }

  case class copyService(resourceName: ResourceName) extends SodaResource {
    def snapshotLimit(req: HttpServletRequest) =
      try { Option(req.getParameter("schema")).map(_.toInt) }
      catch { case e: NumberFormatException => ??? /* TODO: Proper error */ }

    // TODO: not GET
    override def get = { req =>
      val doCopyData = req.getParameter("copy_data") == "true"
      response(req, datasetDAO.makeCopy(user(req), resourceName, copyData = doCopyData))
    }

    override def delete = { req =>
      response(req, datasetDAO.dropCurrentWorkingCopy(user(req), resourceName))
    }
    override def put = { req =>
      response(req, datasetDAO.publish(user(req), resourceName, snapshotLimit = snapshotLimit(req)))
    }
  }

  case class versionService(resourceName: ResourceName, secondary: SecondaryId) extends SodaResource {
    override def get = { req =>
      response(req, datasetDAO.getVersion(resourceName, secondary))
    }
  }

  case class secondaryCopyService(resourceName: ResourceName, secondary: SecondaryId) extends SodaResource {
    override def post = { req =>
      response(req, datasetDAO.propagateToSecondary(resourceName, secondary))
    }
  }

  case class rollupService(resourceName: ResourceName, rollupName: RollupName) extends SodaResource {
    override def get = { req =>
      // TODO Not implemented yet
//      response(req, datasetDAO.getRollup(user(req), resourceName, rollupName))
      MethodNotAllowed
    }

    override def delete = { req =>
      response(req, datasetDAO.deleteRollup(user(req), resourceName, rollupName))
    }

    override def put = { req =>
      withRollupSpec(req) { spec =>
        response(req, datasetDAO.replaceOrCreateRollup(user(req), resourceName, rollupName, spec))
      }
    }
  }
}
