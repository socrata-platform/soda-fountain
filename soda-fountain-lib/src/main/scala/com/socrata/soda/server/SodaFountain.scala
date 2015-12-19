package com.socrata.soda.server

import com.mchange.v2.c3p0.DataSources
import com.netflix.astyanax.AstyanaxContext
import com.rojoma.simplearm.v2.{Resource => SAResource, ResourceScope}
import com.socrata.geocoders._
import com.socrata.geocoders.caching.{NoopCacheClient, CassandraCacheClient}
import com.socrata.http.client.{InetLivenessChecker, HttpClientHttpClient}
import com.socrata.http.common.AuxiliaryData
import com.socrata.http.server.util.RequestId
import com.socrata.http.server.util.handlers.{NewLoggingHandler, ThreadRenamingHandler}
import com.socrata.http.server.util.RequestId.ReqIdHeader
import com.socrata.soda.clients.datacoordinator.{CuratedHttpDataCoordinatorClient, DataCoordinatorClient}
import com.socrata.soda.clients.regioncoder.CuratedRegionCoderClient
import com.socrata.soda.clients.querycoordinator.{CuratedHttpQueryCoordinatorClient, QueryCoordinatorClient}
import com.socrata.soda.server.computation.ComputedColumns
import com.socrata.soda.server.config.{GeocodingConfig, SodaFountainConfig}
import com.socrata.soda.server.highlevel._
import com.socrata.soda.server.metrics.Metrics.{Metric, GeocodeMetric, MapQuestGeocodeMetric}
import com.socrata.soda.server.persistence.pg.PostgresStoreImpl
import com.socrata.soda.server.persistence.{DataSourceFromConfig, NameAndSchemaStore}
import com.socrata.soda.server.metrics.{BalboaMetricProvider, NoopMetricProvider}
import com.socrata.soda.server.util._
import com.socrata.curator.{CuratorFromConfig, DiscoveryFromConfig}
import com.socrata.thirdparty.astyanax.AstyanaxFromConfig
import com.socrata.thirdparty.typesafeconfig.Propertizer
import java.io.Closeable
import java.security.SecureRandom
import java.util.concurrent.{CountDownLatch, TimeUnit, Executors}
import javax.sql.DataSource
import org.apache.log4j.PropertyConfigurator

/**
 * Manages the lifecycle of the routing table.  This means that
 * it intializes resources that are necessary across the lifetime
 * of the server for the use of services, knows the routing table,
 * and cleans up the resources on shutdown.
 */
class SodaFountain(config: SodaFountainConfig, parentResourceScope: ResourceScope) extends Closeable {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[SodaFountain])

  PropertyConfigurator.configure(Propertizer("log4j", config.log4j))

  val logOptions = NewLoggingHandler.defaultOptions.copy(
                     logRequestHeaders = Set(ReqIdHeader),
                     logResponseHeaders = Set(QueryCoordinatorClient.HeaderRollup))

  val handle =
    ThreadRenamingHandler {
      NewLoggingHandler(logOptions) { req =>
        val httpResponse = try {
          router.route(req)
        } catch {
          case e: Throwable if !e.isInstanceOf[Error] =>
            SodaUtils.internalError(req, e)
        }

      { resp =>
        try {
          httpResponse(resp)
        } catch {
          case e: Throwable if !e.isInstanceOf[Error] =>
            if (!resp.isCommitted) {
              resp.reset()
              SodaUtils.internalError(req, e)(resp)
            } else {
              log.warn("Caught exception but the response is already committed; just cutting the client off" +
                "\n" + e.getMessage, e)
            }
        }
      }
      }
      }


  // Below this line is all setup.
  // Note: all initialization that can possibly throw should
  // either go ABOVE the declaration of "cleanup" or be guarded
  // by i() or si() to ensure things are cleaned up if something
  // goes wrong.

  val rng = new scala.util.Random(new SecureRandom())
  val columnSpecUtils = new ColumnSpecUtils(rng)

  // If something in this constructor throws an exception, it'll be closed by this
  // ResourceScope, which is managed over in "main".  It's not _quite_ prompt closure
  // (the resources won't be closed before the exception leaves the constructor) but it's
  // close enough.
  private val resourceScope = parentResourceScope.open(new ResourceScope("soda-fountain"))

  private def i[T : SAResource](thing: => T): T = {
    var done = false
    try {
      val result = resourceScope.open(thing)
      done = true
      result
    } finally {
      if(!done) close()
    }
  }

  private type Startable = { def start(): Unit }
  private def si[T <: Startable : SAResource](thing: => T): T = {
    import scala.language.reflectiveCalls
    val res = i(thing)
    var done = false
    try {
      res.start()
      done = true
    } finally {
      if(!done) close()
    }
    res
  }

  val curator = si(CuratorFromConfig.unmanaged(config.curator))

  val discovery = si(DiscoveryFromConfig.unmanaged(classOf[AuxiliaryData], curator, config.discovery))

  val executor = i(new CloseableExecutorService(Executors.newCachedThreadPool()))

  val livenessChecker = si(new InetLivenessChecker(config.network.httpclient.liveness.interval, config.network.httpclient.liveness.range, config.network.httpclient.liveness.missable, executor, rng))

  val httpClient = i(new HttpClientHttpClient(executor,
    HttpClientHttpClient.defaultOptions.
      withLivenessChecker(livenessChecker).
      withUserAgent("soda fountain")))

  val dc: DataCoordinatorClient = i(new CuratedHttpDataCoordinatorClient(
    httpClient,
    discovery,
    config.dataCoordinatorClient.serviceName,
    config.dataCoordinatorClient.instance,
    config.dataCoordinatorClient.connectTimeout,
    config.dataCoordinatorClient.receiveTimeout))

  val qc: QueryCoordinatorClient = si(new CuratedHttpQueryCoordinatorClient(
    httpClient,
    discovery,
    config.queryCoordinatorClient.serviceName,
    config.queryCoordinatorClient.connectTimeout,
    config.queryCoordinatorClient.receiveTimeout))

  val regionCoder = si(new CuratedRegionCoderClient(discovery, config.regionCoderClient))

  val metricProvider = config.metrics.map( balboaConfig => si(new BalboaMetricProvider(balboaConfig)) ).getOrElse(new NoopMetricProvider)

  private implicit def astyanaxResource[T] = new SAResource[AstyanaxContext[T]] {
    def close(k: AstyanaxContext[T]) = k.shutdown()
  }
  val astyanax = config.cassandra.map { cassCfg => si(AstyanaxFromConfig.unmanaged(cassCfg)) }

  def geocoder: ((Metric => Unit) => Geocoder) = locally { metrizer: (Metric => Unit) =>
    val geoConfig = config.geocodingProvider

    def countMetric: (Long => Unit) = { count: Long =>
      metrizer(GeocodeMetric.count(count.toInt))
    }

    def cachedMetric: (Long => Unit) = { count: Long =>
      metrizer(GeocodeMetric.cached(count.toInt))
    }

    def mapQuestMetric: ((GeocodingResult, Long) => Unit) = { (result: GeocodingResult, count: Long) =>
      result match {
        case SuccessResult => metrizer(MapQuestGeocodeMetric.success(count.toInt))
        case InsufficientlyPreciseResult => metrizer(MapQuestGeocodeMetric.insufficientlyPreciseResult(count.toInt))
        case UninterpretableResult => metrizer(MapQuestGeocodeMetric.uninterpretableResult(count.toInt))
      }
    }

    def geocoderProvider: Geocoder = {
      val geocoder = geoConfig match {
        case Some(geoCfg) =>
          val base = baseGeocoderProvider(geoCfg)
          cachingGeocoderProvider(geoCfg, base)
        case None =>
          log.info("No geocoder-provider configuration provided; using NoopGeocoder.")
          NoopGeocoder
      }
      new GeocoderMetricizer(countMetric, geocoder)
    }

    def baseGeocoderProvider(config: GeocodingConfig): Geocoder = config.mapQuest match {
      case Some(e) =>
        new MapQuestGeocoder(httpClient, e.appToken, mapQuestMetric, e.retryCount)
      case None =>
        log.info("No MapQuest configuration provided; using NoopGeocoder.")
        NoopGeocoder
    }

    def cachingGeocoderProvider(config: GeocodingConfig, base: Geocoder): Geocoder = {
      val cache = config.cache match {
        case Some(cacheConfig) =>
          astyanax match {
            case Some(ast) =>
              new CassandraCacheClient (ast.getClient, cacheConfig.columnFamily, cacheConfig.ttl)
            case None =>
              log.info("No Cassandra configuration provided; using NoopCacheClient for geocoding.")
              NoopCacheClient
          }
        case None =>
          log.info("No Cassandra cache client configuration provided; using NoopCacheClient for geocoding.")
          NoopCacheClient
      }
      new CachingGeocoderAdapter(cache, base, cachedMetric, config.filterMultipier)
    }

    geocoderProvider
  }

  implicit def dsResource = new SAResource[DataSource] {
    def close(ds: DataSource) = DataSources.destroy(ds)
  }
  val dataSource = i(DataSourceFromConfig(config.database))

  val store: NameAndSchemaStore = new PostgresStoreImpl(dataSource)

  def computedColumns(metrizer: (Metric => Unit)) = new ComputedColumns(config.handlers, discovery, geocoder(metrizer), metrizer)

  val datasetDAO = new DatasetDAOImpl(dc, store, columnSpecUtils, () => config.dataCoordinatorClient.instance)
  val columnDAO = new ColumnDAOImpl(dc, store, columnSpecUtils)
  val rowDAO = new RowDAOImpl(store, dc, qc)
  val exportDAO = new ExportDAOImpl(store, dc)

  val etagObfuscator = config.etagObfuscationKey.fold(ETagObfuscator.noop) { key => new BlowfishCFBETagObfuscator(key.getBytes("UTF-8")) }

  val tableDropDelay = config.tableDropDelay
  val dataCleanupInterval = config.dataCleanupInterval
  val router = locally {
    import com.socrata.soda.server.resources._

    val healthZ = HealthZ(regionCoder)
    // TODO: this should probably be a different max size value
    val resource = Resource(rowDAO, datasetDAO, etagObfuscator, config.maxDatumSize, computedColumns, metricProvider)
    val dataset = Dataset(datasetDAO, config.maxDatumSize)
    val column = DatasetColumn(columnDAO, exportDAO, rowDAO, computedColumns, metricProvider, etagObfuscator, config.maxDatumSize)
    val export = Export(exportDAO, etagObfuscator)
    val compute = Compute(columnDAO, exportDAO, rowDAO, computedColumns, metricProvider, etagObfuscator)
    val suggest = Suggest(datasetDAO, columnDAO, httpClient, config.suggest)

    new SodaRouter(
      versionResource = Version.service,
      healthZResource = healthZ.service,
      datasetColumnResource = column.service,
      datasetColumnPKResource = column.pkservice,
      datasetCreateResource = dataset.createService,
      datasetResource = dataset.service,
      resourceResource = resource.service,
      resourceExtensions = resource.extensions,
      resourceRowResource = resource.rowService,
      datasetCopyResource = dataset.copyService,
      datasetSecondaryCopyResource = dataset.secondaryCopyService,
      datasetVersionResource = dataset.versionService,
      datasetExportResource = export.publishedService,
      datasetExportCopyResource = export.service,
      exportExtensions = export.extensions,
      datasetRollupResource = dataset.rollupService,
      computeResource = compute.service,
      sampleResource = suggest.sampleService,
      suggestResource = suggest.service
    )
  }

  //For each of the datasets, call a delete function on each one of them
  //Remove datasets from truth and secondary and sodafountain
  val finished = new CountDownLatch(1)
  val tableDropper = new Thread() {
    setName("table dropper")

    override def run() {
      do {
        try {
          val records = store.lookupDroppedDatasets(tableDropDelay)
          records.foreach { rec =>
            log.info(s"Dropping dataset ${rec.resourceName} (${rec.systemId}")
            datasetDAO.removeDataset("", rec.resourceName, RequestId.generate())
          }
          //call data coordinator to remove datasets in truth
        }
        catch {
          case e: Exception =>
            log.error("Unexpected error while cleaning tables", e)
        }
      } while (!finished.await(dataCleanupInterval, TimeUnit.SECONDS))
    }
  }

  def close() {
    parentResourceScope.close(resourceScope)
  }
}
