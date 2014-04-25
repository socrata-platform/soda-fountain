package com.socrata.soda.server.resources

import com.socrata.http.server.HttpResponse
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.soda.server.errors.InternalError
import com.socrata.soda.server.highlevel.DatasetDAO
import com.socrata.soda.server.id.{SecondaryId, ResourceName}
import com.socrata.soda.server.wiremodels.{IOProblem, RequestProblem, Extracted, UserProvidedDatasetSpec}
import com.socrata.soda.server.{SodaUtils, LogTag}
import javax.servlet.http.HttpServletRequest

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

  def response(result: DatasetDAO.Result): HttpResponse = {
    log.info("TODO: Negotiate content-type")
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
    }
  }

  object createService extends SodaResource {
    override def post = { req =>
      withDatasetSpec(req) { spec =>
        response(datasetDAO.createDataset(user(req), spec))
      }
    }
  }

  case class service(resourceName: ResourceName) extends SodaResource {
    override def put = { req =>
      withDatasetSpec(req, resourceName) { spec =>
        response(datasetDAO.replaceOrCreateDataset(user(req), resourceName, spec))
      }
    }

    override def get = { req =>
      response(datasetDAO.getDataset(resourceName))
    }

    override def patch = { req =>
      withDatasetSpec(req, resourceName) { spec =>
        response(datasetDAO.updateDataset(user(req), resourceName, spec))
      }
    }

    override def delete = { req =>
      response(datasetDAO.deleteDataset(user(req), resourceName))
    }
  }

  case class copyService(resourceName: ResourceName) extends SodaResource {
    def snapshotLimit(req: HttpServletRequest) =
      try { Option(req.getParameter("schema")).map(_.toInt) }
      catch { case e: NumberFormatException => ??? /* TODO: Proper error */ }

    // TODO: not GET
    override def get = { req =>
      val doCopyData = req.getParameter("copy_data") == "true"
      response(datasetDAO.makeCopy(user(req), resourceName, copyData = doCopyData))
    }

    override def delete = { req =>
      response(datasetDAO.dropCurrentWorkingCopy(user(req), resourceName))
    }
    override def put = { req =>
      response(datasetDAO.publish(user(req), resourceName, snapshotLimit = snapshotLimit(req)))
    }
  }

  case class versionService(resourceName: ResourceName, secondary: SecondaryId) extends SodaResource {
    override def get = { req =>
      response(datasetDAO.getVersion(resourceName, secondary))
    }
  }

  case class secondaryCopyService(resourceName: ResourceName, secondary: SecondaryId) extends SodaResource {
    override def post = { req =>
      response(datasetDAO.propagateToSecondary(resourceName, secondary))
    }
  }
}
