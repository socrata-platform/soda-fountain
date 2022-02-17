package com.socrata.soda.server.resources

import com.rojoma.json.v3.ast.JString
import com.socrata.http.server.{HttpRequest, HttpResponse}
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.util.RequestId
import com.socrata.soda.server._
import com.socrata.soda.server.copy.Stage
import com.socrata.soda.server.responses._
import com.socrata.soda.server.highlevel._
import com.socrata.soda.server.highlevel.DatasetDAO
import com.socrata.soda.server.id.{ResourceName, RollupName, SecondaryId}
import com.socrata.soda.server.wiremodels.{Extracted, UserProvidedDatasetSpec, UserProvidedRollupSpec}
import com.socrata.soda.server.wiremodels.{IOProblem, RequestProblem}
import javax.servlet.http.HttpServletRequest

import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder

/**
 * Dataset: CRUD operations for dataset schema and metadata
 */
case class Dataset(maxDatumSize: Int) {

  val schemaHashHeaderName = "x-socrata-version-hash"
  val log = org.slf4j.LoggerFactory.getLogger(classOf[Dataset])

  def withDatasetSpec(request: SodaRequest, logTags: LogTag*)(f: UserProvidedDatasetSpec => HttpResponse): HttpResponse = {
    UserProvidedDatasetSpec.fromRequest(request.httpRequest, maxDatumSize) match {
      case Extracted(datasetSpec) =>
        f(datasetSpec)
      case RequestProblem(err) =>
        SodaUtils.response(request, err, logTags : _*)
      case IOProblem(err) =>
        SodaUtils.internalError(request, err)
    }
  }

  def withRollupSpec(request: HttpRequest, logTags: LogTag*)(f: UserProvidedRollupSpec => HttpResponse): HttpResponse = {
    UserProvidedRollupSpec.fromRequest(request, maxDatumSize) match {
      case Extracted(datasetSpec) =>
        f(datasetSpec)
      case RequestProblem(err) =>
        SodaUtils.response(request, err, logTags : _*)
      case IOProblem(err) =>
        SodaUtils.internalError(request, err)
    }
  }

  private def dataVersionHeaders(data: Long, shape: Long) =
    Header("X-SODA2-Truth-Version", data.toString) ~>
      Header("X-SODA2-Truth-Shape-Version", shape.toString)

  def response(req: SodaRequest, result: DatasetDAO.Result): HttpResponse = {
    // TODO: Negotiate content type
    log.debug(s"sending response, result: ${result}")
    result match {
      // success cases
      case DatasetDAO.Updated(record) =>
        OK ~> Json(record.asSpec)
      case DatasetDAO.Found(record) =>
        OK ~> Json(record.asSpec)
      case DatasetDAO.DatasetSecondaryVersions(vrs) =>
        OK ~> Json(vrs)
      case DatasetDAO.DatasetVersion(vr) =>
        OK ~> Json(vr)
      case DatasetDAO.Created(record) =>
        Created ~> Json(record.asSpec)
      case DatasetDAO.WorkingCopyCreated(data, shape) =>
        Created ~> dataVersionHeaders(data, shape)
      case DatasetDAO.PropagatedToSecondary =>
        Created
      case DatasetDAO.WorkingCopyDropped(data, shape) =>
        NoContent ~> dataVersionHeaders(data, shape)
      case DatasetDAO.WorkingCopyPublished(data, shape) =>
        NoContent ~> dataVersionHeaders(data, shape)
      case DatasetDAO.RollupCreatedOrUpdated =>
        NoContent
      case DatasetDAO.Deleted =>
        NoContent
      case DatasetDAO.RollupDropped =>
        NoContent
      case DatasetDAO.Undeleted =>
        NoContent
      case DatasetDAO.EmptyResult =>
        NoContent
      case DatasetDAO.Rollups(rollups) =>
        OK ~> Json(rollups)
      case collocateResult: DatasetDAO.CollocateDone =>
        implicit val codec = AutomaticJsonCodecBuilder[DatasetDAO.CollocateDone]
        OK ~> Json(collocateResult)
      // fail cases
      case DatasetDAO.DatasetAlreadyExists(dataset) =>
        SodaUtils.response(req, DatasetAlreadyExistsSodaErr(dataset))
      case DatasetDAO.NonExistentColumn(dataset, column) =>
        SodaUtils.response(req, ColumnNotFound(dataset, column))
      case DatasetDAO.LocaleChanged(locale) =>
        SodaUtils.response(req, LocaleChangedError(locale))
      case DatasetDAO.DatasetNotFound(dataset) =>
        SodaUtils.response(req, DatasetNotFound(dataset))
      case DatasetDAO.DatasetVersionMismatch(dataset, version) =>
        SodaUtils.response(req, DatasetVersionMismatch(dataset, version))
      case DatasetDAO.InvalidDatasetName(name) =>
        SodaUtils.response(req, DatasetNameInvalidNameSodaErr(name))
      case DatasetDAO.RollupNotFound(name) =>
        SodaUtils.response(req, RollupNotFound(name))
      case DatasetDAO.RollupError(reason) =>
        SodaUtils.response(req, RollupCreationFailed(reason))
      case DatasetDAO.RollupColumnNotFound(column) =>
        SodaUtils.response(req, RollupColumnNotFound(column))
      case DatasetDAO.CannotAcquireDatasetWriteLock(dataset) =>
        SodaUtils.response(req, DatasetWriteLockError(dataset))
      case DatasetDAO.FeedbackInProgress(dataset, stores) =>
        SodaUtils.response(req, FeedbackInProgressError(dataset, stores))
      case DatasetDAO.IncorrectLifecycleStageResult(actualStage: String, expectedStage: Set[String]) =>
        SodaUtils.response(req, IncorrectLifecycleStage(actualStage, expectedStage))
      case DatasetDAO.UnsupportedUpdateOperation(message) =>
        SodaUtils.response(req, UnsupportedUpdateOperation(message))
      case DatasetDAO.InternalServerError(code, tag, data) =>
        SodaUtils.response(req, InternalError(tag, "code"  -> JString(code), "data" -> JString(data)))
      case DatasetDAO.UnexpectedInternalServerResponse(reason, tag) =>
        SodaUtils.response(req, InternalError(tag, "reason"  -> JString(reason)))
      case DatasetDAO.GenericCollocateError(err) =>
        SodaUtils.response(req, CollocateError(err))

    }
  }

  object createService extends SodaResource {
    override def post = { req =>
      withDatasetSpec(req) { spec =>
        response(req, req.datasetDAO.createDataset(user(req), spec))
      }
    }
  }

  case class service(resourceName: ResourceName) extends SodaResource {
    override def put = { req =>
      withDatasetSpec(req, resourceName) { spec =>
        response(req, req.datasetDAO.replaceOrCreateDataset(user(req), resourceName, spec))
      }
    }

    override def get = { req =>
      val copy = Stage(req.queryParameter("$$copy")) // Query parameter for copy.  Optional, "latest", "published", "unpublished", or "latest"
      response(req, req.datasetDAO.getDataset(resourceName, copy))
    }

    override def patch = { req =>
      withDatasetSpec(req, resourceName) { spec =>
        response(req, req.datasetDAO.updateDataset(user(req), resourceName, spec))
      }
    }

    override def delete = { req =>
      response(req, req.datasetDAO.markDatasetForDeletion(user(req), resourceName))
    }
  }

  case class undeleteService(resourceName: ResourceName) extends SodaResource {
    override def post = { req =>
      response(req, req.datasetDAO.unmarkDatasetForDeletion(user(req), resourceName))
    }
  }

  case class secondaryReindexService(resourceName: ResourceName) extends SodaResource {
    override def post = { req =>
      withDatasetSpec(req, resourceName) { spec =>
        response(req, req.datasetDAO.secondaryReindex(user(req), resourceName, expectedDataVersion(req)))
      }
    }
  }

  case class copyService(resourceName: ResourceName) extends SodaResource {
    def keepSnapshot(req: SodaRequest) =
      try { req.queryParameter("keep_snapshot").map(_.toBoolean) }
      catch { case e: IllegalArgumentException => ??? /* TODO: Proper error */ }

    // TODO: not GET
    override def get = { req =>
      val doCopyData = req.queryParameter("copy_data") == Some("true")
      response(req, req.datasetDAO.makeCopy(user(req), resourceName, expectedDataVersion(req), copyData = doCopyData))
    }

    override def delete = { req =>
      response(req, req.datasetDAO.dropCurrentWorkingCopy(user(req), resourceName, expectedDataVersion(req)))
    }
    override def put = { req =>
      response(req, req.datasetDAO.publish(user(req), resourceName, expectedDataVersion(req), keepSnapshot = keepSnapshot(req)))
    }
  }

  case class secondaryVersionsService(resource: ResourceName) extends SodaResource {
    override def get = { req =>
      response(req, req.datasetDAO.getSecondaryVersions(resource))
    }
  }

  case class versionService(resourceName: ResourceName, secondary: SecondaryId) extends SodaResource {
    override def get = { req =>
      response(req, req.datasetDAO.getVersion(resourceName, secondary))
    }
  }

  case class secondaryCopyService(resourceName: ResourceName, secondary: SecondaryId) extends SodaResource {
    override def post = { req =>
      val secondariesLike = req.queryParameter("secondaries_like").map(x => new ResourceName(x))
      response(req, req.datasetDAO.propagateToSecondary(resourceName, secondary, secondariesLike))
    }
  }

  case class secondaryCollocateService(secondaryId: SecondaryId) extends SodaResource{
    override def post = { req =>
       SFCollocateOperation.getFromRequest(req.httpRequest) match {
        case Right(operation) =>
          val explain = req.queryParameter("explain") == Some("true")

          // TODO: Proper error handling
          val jobId = req.queryParameter("job").getOrElse(throw new Exception)
          response(req, req.datasetDAO.collocate(secondaryId, operation, explain, jobId))
        case Left(_) =>
          // NOTE: Is there a better error I could be throwing?
          BadRequest
      }
    }
  }

  case class secondaryCollocateJobService(resourceName: ResourceName, secondaryId: SecondaryId, jobId: String) extends SodaResource {
    override def get = { req =>
      response(req, req.datasetDAO.collocateStatus(resourceName, secondaryId, jobId))
    }

    override def delete = { req =>
      response(req, req.datasetDAO.deleteCollocate(resourceName, secondaryId, jobId))
    }
  }

  case class rollupService(resourceName: ResourceName, rollupName: Option[RollupName]) extends SodaResource {
    override def get = { req =>
      rollupName match {
        case Some(_) => MethodNotAllowed
        case None =>
          response(req, req.datasetDAO.getRollups(resourceName))
      }
    }

    override def delete = { req =>
      rollupName match {
        case Some(rollup) =>
          response(req, req.datasetDAO.deleteRollup(user(req), resourceName, expectedDataVersion(req), rollup))
        case None => MethodNotAllowed
      }
    }

    override def put = { req =>
      withRollupSpec(req.httpRequest) { spec =>
        rollupName match {
          case Some(rollup) =>
            response(req, req.datasetDAO.replaceOrCreateRollup(user(req), resourceName, expectedDataVersion(req), rollup, spec))
          case None => MethodNotAllowed
        }

      }
    }
  }
}
