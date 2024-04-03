package com.socrata.soda.server

import com.socrata.http.server.implicits._
import com.socrata.http.server.routing.RouteContext
import com.socrata.http.server.routing.{Extractor, OptionallyTypedPathComponent}
import com.socrata.http.server.{HttpResponse}
import com.socrata.soda.server.responses.GeneralNotFoundError
import com.socrata.soda.server.id.{IndexName, ResourceName, RollupName, RowSpecifier, SecondaryId}
import com.socrata.soql.environment.ColumnName

class SodaRouter(versionResource: SodaHttpService,
                 healthZResource: SodaHttpService,
                 datasetCreateResource: SodaHttpService,
                 datasetResource: ResourceName => SodaHttpService,
                 datasetUndeleteResource: ResourceName => SodaHttpService,
                 datasetColumnResource: (ResourceName, ColumnName) => SodaHttpService,
                 datasetColumnPKResource: (ResourceName, ColumnName) => SodaHttpService,
                 resourceResource: OptionallyTypedPathComponent[ResourceName] => SodaHttpService,
                 resourceExtensions: String => Boolean,
                 resourceRowResource: (ResourceName, RowSpecifier) => SodaHttpService,
                 newResourceResource: SodaHttpService,
                 newRollupResource: SodaHttpService,
                 resyncResource: (ResourceName, SecondaryId) => SodaHttpService,
                 datasetCopyResource: ResourceName => SodaHttpService,
                 datasetSecondaryCopyResource: (ResourceName, SecondaryId) => SodaHttpService,
                 datasetSecondaryCollocateResource: (SecondaryId) => SodaHttpService,
                 datasetSecondaryCollocateJobResource: (ResourceName, SecondaryId, String) => SodaHttpService,
                 datasetSecondaryVersionsResource: (ResourceName) => SodaHttpService,
                 datasetVersionResource: (ResourceName, SecondaryId) => SodaHttpService,
                 datasetExportResource: OptionallyTypedPathComponent[ResourceName] => SodaHttpService,
                 datasetExportCopyResource: (ResourceName, OptionallyTypedPathComponent[String]) => SodaHttpService,
                 exportExtensions: String => Boolean,
                 datasetRollupsResource: ResourceName => SodaHttpService,
                 datasetRollupResource: (ResourceName, RollupName) => SodaHttpService,
                 datasetRollupRelationFromResource: (ResourceName) => SodaHttpService,
                 datasetRollupRelationToResource: (ResourceName) => SodaHttpService,
                 datasetIndexesResource: ResourceName => SodaHttpService,
                 datasetIndexResource: (ResourceName, IndexName) => SodaHttpService,
                 secondaryReindexResource: ResourceName => SodaHttpService,
                 indexDirectiveResource: (ResourceName, ColumnName) => SodaHttpService) {
  private[this] val routeContext = new RouteContext[SodaRequest, HttpResponse]
  import routeContext._

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

  private[this] implicit val IndexNameExtractor = new Extractor[IndexName] {
    def extract(s: String): Option[IndexName] = Some(new IndexName(s))
  }

  // NOTE: until there's something like Swagger, document routes with comments
  val router = Routes(
    // Health checks
    Route("/version", versionResource),
    Route("/healthz", healthZResource),
    // Dataset schema and metadata CRUD operations
    Route("/dataset", datasetCreateResource),
    Route("/dataset/{ResourceName}", datasetResource),
    Route("/dataset/{ResourceName}/{ColumnName}", datasetColumnResource),
    Route("/dataset/{ResourceName}/{ColumnName}/index-directive", indexDirectiveResource),
    Route("/dataset/{ResourceName}/{ColumnName}/!makepk", datasetColumnPKResource), // hack hack; once full DDL is implemented this can go away
    Route("/dataset/", datasetCreateResource),
    Route("/dataset/{ResourceName}/!secondary-reindex", secondaryReindexResource),
    Route("/dataset/undelete/{ResourceName}", datasetUndeleteResource),
    Directory("/resource"),
    // resource: CRUD operations and queries on dataset records and rows
    Route("/resource/{{ResourceName:resourceExtensions}}", resourceResource),
    Route("/resource/{ResourceName}/{RowSpecifier}", resourceRowResource),
    Route("/new-resource.cjson", newResourceResource),
    Route("/new-resource/rollup", newRollupResource),
    Route("/dataset-copy/{ResourceName}", datasetCopyResource),
    Route("/dataset-copy/{ResourceName}/{SecondaryId}", datasetSecondaryCopyResource),
    Route("/dataset-copy/collocate/{SecondaryId}", datasetSecondaryCollocateResource),
    Route("/dataset-copy/{ResourceName}/collocate/{SecondaryId}/job/{String}", datasetSecondaryCollocateJobResource),
    Route("/dataset-version/{ResourceName}", datasetSecondaryVersionsResource),
    Route("/dataset-version/{ResourceName}/{SecondaryId}", datasetVersionResource),
    Route("/export/{{ResourceName:exportExtensions}}", datasetExportResource),
    Route("/export/{ResourceName}/{{String:exportExtensions}}", datasetExportCopyResource),
    Route("/dataset-rollup/{ResourceName}", datasetRollupsResource),
    Route("/dataset-rollup/{ResourceName}/{RollupName}", datasetRollupResource),
    Route("/dataset-rollup/{ResourceName}/from", datasetRollupRelationFromResource),
    Route("/dataset-rollup/{ResourceName}/to", datasetRollupRelationToResource),
    Route("/dataset-index/{ResourceName}", datasetIndexesResource),
    Route("/dataset-index/{ResourceName}/{IndexName}", datasetIndexResource),
    Route("/resync/{ResourceName}/{SecondaryId}", resyncResource)
  )

  def route(req: SodaRequest): HttpResponse = {
    router(req.requestPath.map(InputNormalizer.normalize)) match {
      case Some(s) =>
        s(req)
      case None =>
        SodaUtils.response(req, GeneralNotFoundError(req.httpRequest.servletRequest.getRequestURI))
    }
  }
}
