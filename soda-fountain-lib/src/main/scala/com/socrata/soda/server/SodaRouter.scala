package com.socrata.soda.server

import javax.servlet.http.HttpServletRequest
import com.socrata.http.server.{HttpService, HttpResponse}
import com.socrata.http.server.implicits._
import com.socrata.soda.server.errors.GeneralNotFoundError
import com.socrata.http.server.routing.SimpleRouteContext._
import com.socrata.soql.environment.ColumnName
import com.socrata.soda.server.id.{SecondaryId, ResourceName}
import com.socrata.http.server.routing.{OptionallyTypedPathComponent, Extractor}

class SodaRouter(versionResource: HttpService,
                 datasetCreateResource: HttpService,
                 datasetResource: ResourceName => HttpService,
                 datasetColumnResource: (ResourceName, ColumnName) => HttpService,
                 resourceResource: ResourceName => HttpService,
                 resourceRowResource: (ResourceName, String) => HttpService,
                 datasetCopyResource: ResourceName => HttpService,
                 datasetExportResource: OptionallyTypedPathComponent[ResourceName] => HttpService,
                 exportExtensions: String => Boolean)
{
  private[this] implicit val ResourceNameExtractor = new Extractor[ResourceName] {
    def extract(s: String): Option[ResourceName] = Some(new ResourceName(s))
  }

  private[this] implicit val ColumnNameExtractor = new Extractor[ColumnName] {
    def extract(s: String): Option[ColumnName] = Some(new ColumnName(s))
  }

  private[this] implicit val SecondaryIdExtractor = new Extractor[SecondaryId] {
    def extract(s: String) = Some(new SecondaryId(s))
  }

  val router = Routes(
    Route("/version", versionResource),
    Route("/dataset", datasetCreateResource),
    Route("/dataset/{ResourceName}", datasetResource),
    Route("/dataset/{ResourceName}/{ColumnName}", datasetColumnResource),
    Route("/dataset/", datasetCreateResource),
    Directory("/resource"),
    Route("/resource/{ResourceName}", resourceResource),
    Route("/resource/{ResourceName}/?", resourceRowResource),
    Route("/dataset-copy/{ResourceName}", datasetCopyResource),
    Route("/export/{{ResourceName:exportExtensions}}", datasetExportResource)
  )

  def route(req: HttpServletRequest): HttpResponse =
    router(req.requestPath.map(InputNormalizer.normalize)) match {
      case Some(s) =>
        s(req)
      case None =>
        SodaUtils.errorResponse(req, GeneralNotFoundError(req.getRequestURI))
    }
}
