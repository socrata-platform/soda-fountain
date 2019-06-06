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
case class Dataset(datasetDAO: DatasetDAO, maxDatumSize: Int) {

  val schemaHashHeaderName = "x-socrata-version-hash"
  val log = org.slf4j.LoggerFactory.getLogger(classOf[Dataset])

  def withDatasetSpec(request: HttpRequest, logTags: LogTag*)(f: UserProvidedDatasetSpec => HttpResponse): HttpResponse = {
    UserProvidedDatasetSpec.fromRequest(request, maxDatumSize) match {
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

  private def dataVersionHeader(v: Long) =
    Header("X-SODA2-Truth-Version", v.toString)

  def response(req: HttpRequest, result: DatasetDAO.Result): HttpResponse = {
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
      case DatasetDAO.WorkingCopyCreated(v) =>
        Created ~> dataVersionHeader(v)
      case DatasetDAO.PropagatedToSecondary =>
        Created
      case DatasetDAO.WorkingCopyDropped(v) =>
        NoContent ~> dataVersionHeader(v)
      case DatasetDAO.WorkingCopyPublished(v) =>
        NoContent ~> dataVersionHeader(v)
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
      response(req, datasetDAO.markDatasetForDeletion(user(req), resourceName))
    }
  }

  case class undeleteService(resourceName: ResourceName) extends SodaResource {
    override def post = { req =>
      response(req, datasetDAO.unmarkDatasetForDeletion(user(req), resourceName))
    }
  }

  case class secondaryReindexService(resourceName: ResourceName) extends SodaResource {
    override def post = { req =>
      withDatasetSpec(req, resourceName) { spec =>
        response(req, datasetDAO.secondaryReindex(user(req), resourceName, expectedDataVersion(req)))
      }
    }
  }

  case class copyService(resourceName: ResourceName) extends SodaResource {
    def keepSnapshot(req: HttpServletRequest) =
      try { Option(req.getParameter("keep_snapshot")).map(_.toBoolean) }
      catch { case e: IllegalArgumentException => ??? /* TODO: Proper error */ }

    // TODO: not GET
    override def get = { req =>
      val doCopyData = req.getParameter("copy_data") == "true"
      response(req, datasetDAO.makeCopy(user(req), resourceName, expectedDataVersion(req), copyData = doCopyData,
                                        requestId = RequestId.getFromRequest(req)))
    }

    override def delete = { req =>
      response(req, datasetDAO.dropCurrentWorkingCopy(user(req), resourceName, expectedDataVersion(req),
                                                      RequestId.getFromRequest(req)))
    }
    override def put = { req =>
      response(req, datasetDAO.publish(user(req), resourceName, expectedDataVersion(req), keepSnapshot = keepSnapshot(req),
                                       requestId = RequestId.getFromRequest(req)))
    }
  }

  case class secondaryVersionsService(resource: ResourceName) extends SodaResource {
    override def get = { req =>
      response(req, datasetDAO.getSecondaryVersions(resource, RequestId.getFromRequest(req)))
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

  case class secondaryCollocateService(secondaryId: SecondaryId) extends SodaResource{
    override def post = { req =>
       SFCollocateOperation.getFromRequest(req) match {
        case Right(operation) =>
          val explain = req.getParameter("explain") == "true"

          // TODO: Proper error handling
          val jobId = Option(req.getParameter("job")).getOrElse(throw new Exception)
          response(req, datasetDAO.collocate(secondaryId, operation, explain, jobId))
        case Left(_) =>
          // NOTE: Is there a better error I could be throwing?
          BadRequest
      }
    }
  }

  case class secondaryCollocateJobService(resourceName: ResourceName, secondaryId: SecondaryId, jobId: String) extends SodaResource {
    override def get = { req =>
      response(req, datasetDAO.collocateStatus(resourceName, secondaryId, jobId))
    }

    override def delete = { req =>
      response(req, datasetDAO.deleteCollocate(resourceName, secondaryId, jobId))
    }
  }

  case class rollupService(resourceName: ResourceName, rollupName: Option[RollupName]) extends SodaResource {
    override def get = { req =>
      rollupName match {
        case Some(_) => MethodNotAllowed
        case None =>
          response(req, datasetDAO.getRollups(resourceName, RequestId.getFromRequest(req)))
      }
    }

    override def delete = { req =>
      rollupName match {
        case Some(rollup) =>
          response(req, datasetDAO.deleteRollup(user(req), resourceName, expectedDataVersion(req), rollup,
            RequestId.getFromRequest(req)))
        case None => MethodNotAllowed
      }
    }

    override def put = { req =>
      withRollupSpec(req) { spec =>
        rollupName match {
          case Some(rollup) =>
            response(req, datasetDAO.replaceOrCreateRollup(user(req), resourceName, expectedDataVersion(req), rollup, spec,
              RequestId.getFromRequest(req)))
          case None => MethodNotAllowed
        }

      }
    }
  }
}
