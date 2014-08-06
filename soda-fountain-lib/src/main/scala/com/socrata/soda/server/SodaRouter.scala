package com.socrata.soda.server

import com.socrata.http.server.implicits._
import com.socrata.http.server.routing.SimpleRouteContext._
import com.socrata.http.server.routing.{OptionallyTypedPathComponent, Extractor}
import com.socrata.http.server.{HttpService, HttpResponse}
import com.socrata.soda.server.errors.GeneralNotFoundError
import com.socrata.soda.server.id.{RollupName, RowSpecifier, SecondaryId, ResourceName}
import com.socrata.soql.environment.ColumnName
import javax.servlet.http.HttpServletRequest

class SodaRouter(versionResource: HttpService,
                 healthZResource: HttpService,
                 datasetCreateResource: HttpService,
                 datasetResource: ResourceName => HttpService,
                 datasetColumnResource: (ResourceName, ColumnName) => HttpService,
                 datasetColumnPKResource: (ResourceName, ColumnName) => HttpService,
                 resourceResource: OptionallyTypedPathComponent[ResourceName] => HttpService,
                 resourceExtensions: String => Boolean,
                 resourceRowResource: (ResourceName, RowSpecifier) => HttpService,
                 datasetCopyResource: ResourceName => HttpService,
                 datasetSecondaryCopyResource: (ResourceName, SecondaryId) => HttpService,
                 datasetVersionResource: (ResourceName, SecondaryId) => HttpService,
                 datasetExportResource: OptionallyTypedPathComponent[ResourceName] => HttpService,
                 datasetExportCopyResource: (ResourceName, OptionallyTypedPathComponent[String]) => HttpService,
                 exportExtensions: String => Boolean,
                 datasetRollupResource: (ResourceName, RollupName) => HttpService)
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

  private[this] implicit val RowSpecifierExtractor = new Extractor[RowSpecifier] {
    def extract(s: String) = Some(new RowSpecifier(s))
  }

  private[this] implicit val RollupNameExtractor = new Extractor[RollupName] {
    def extract(s: String): Option[RollupName] = Some(new RollupName(s))
  }

  // NOTE: until there's something like Swagger, document routes with comments
  val router = Routes(
    Route("/version", versionResource),
    Route("/healthz", healthZResource),
    // Dataset schema and metadata CRUD operations
    Route("/dataset", datasetCreateResource),
    Route("/dataset/{ResourceName}", datasetResource),
    Route("/dataset/{ResourceName}/{ColumnName}", datasetColumnResource),
    Route("/dataset/{ResourceName}/{ColumnName}/!makepk", datasetColumnPKResource), // hack hack; once full DDL is implemented this can go away
    Route("/dataset/", datasetCreateResource),
    Directory("/resource"),
    // resource: CRUD operations and queries on dataset records and rows
    Route("/resource/{{ResourceName:resourceExtensions}}", resourceResource),
    Route("/resource/{ResourceName}/{RowSpecifier}", resourceRowResource),
    Route("/dataset-copy/{ResourceName}", datasetCopyResource),
    Route("/dataset-copy/{ResourceName}/{SecondaryId}", datasetSecondaryCopyResource),
    Route("/dataset-version/{ResourceName}/{SecondaryId}", datasetVersionResource),
    Route("/export/{{ResourceName:exportExtensions}}", datasetExportResource),
    Route("/export/{ResourceName}/{{String:exportExtensions}}", datasetExportCopyResource),
    Route("/dataset-rollup/{ResourceName}/{RollupName}", datasetRollupResource)
  )

  def route(req: HttpServletRequest): HttpResponse =
    router(req.requestPath.map(InputNormalizer.normalize)) match {
      case Some(s) =>
        s(req)
      case None =>
        SodaUtils.errorResponse(req, GeneralNotFoundError(req.getRequestURI))
    }
}
