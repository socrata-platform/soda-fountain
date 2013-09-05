package com.socrata.soda.server.resources

import com.socrata.soda.server.id.ResourceName
import com.socrata.soda.server.highlevel.DatasetDAO
import javax.servlet.http.HttpServletRequest
import com.socrata.soda.server.{SodaUtils, LogTag}
import com.socrata.soda.server.wiremodels.{RequestProblem, Extracted, UserProvidedDatasetSpec}
import com.socrata.http.server.HttpResponse
import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._

case class Dataset(datasetDAO: DatasetDAO, maxDatumSize: Int) {
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
      case DatasetDAO.Deleted => NoContent
      case DatasetDAO.NotFound(dataset) => NotFound /* TODO: content */
      case DatasetDAO.InvalidDatasetName(name) => BadRequest /* TODO: content */
      case DatasetDAO.InvalidColumnName(name) => BadRequest /* TODO: content */
    }
  }

  case class service(resourceName: ResourceName) extends SodaResource {
    override def put = { req =>
      withDatasetSpec(req, resourceName) { spec =>
        response(datasetDAO.replaceOrCreateDataset(resourceName, spec))
      }
    }

    override def get = { req =>
      response(datasetDAO.getDataset(resourceName))
    }

    override def patch = { req =>
      withDatasetSpec(req, resourceName) { spec =>
        response(datasetDAO.updateDataset(resourceName, spec))
      }
    }

    override def delete = { req =>
      response(datasetDAO.deleteDataset(resourceName))
    }
  }
}
