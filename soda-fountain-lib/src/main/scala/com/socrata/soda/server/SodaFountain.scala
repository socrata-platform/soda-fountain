package com.socrata.soda.server

import java.nio.charset.StandardCharsets

import com.mchange.v2.c3p0.DataSources
import com.socrata.computation_strategies.{ComputationStrategy, StrategyType}
import com.socrata.http.client.{HttpClientHttpClient, InetLivenessChecker}
import com.socrata.http.common.AuxiliaryData
import com.socrata.http.common.util.CharsetFor
import com.socrata.http.server.util.RequestId
import com.socrata.http.server.util.handlers.{LoggingOptions, NewLoggingHandler, ThreadRenamingHandler}
import com.socrata.http.server.util.RequestId.ReqIdHeader
import com.socrata.soda.clients.datacoordinator.{CuratedHttpDataCoordinatorClient, DataCoordinatorClient, FeedbackSecondaryManifestClient}
import com.socrata.soda.clients.querycoordinator.{CuratedHttpQueryCoordinatorClient, QueryCoordinatorClient}
import com.socrata.soda.server.config.SodaFountainConfig
import com.socrata.soda.server.highlevel._
import com.socrata.soda.server.id.{ResourceName, SecondaryId}
import com.socrata.soda.server.persistence.pg.PostgresStoreImpl
import com.socrata.soda.server.persistence.{DataSourceFromConfig, NameAndSchemaStore}
import com.socrata.soda.server.metrics.NoopMetricProvider
import com.socrata.soda.server.util._
import com.socrata.curator.{CuratorFromConfig, DiscoveryFromConfig}
import com.socrata.thirdparty.typesafeconfig.Propertizer
import java.io.Closeable
import java.security.SecureRandom
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

import javax.sql.DataSource
import org.apache.log4j.PropertyConfigurator
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.Random

/**
 * Manages the lifecycle of the routing table.  This means that
 * it intializes resources that are necessary across the lifetime
 * of the server for the use of services, knows the routing table,
 * and cleans up the resources on shutdown.
 */
class SodaFountain(config: SodaFountainConfig) extends Closeable {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[SodaFountain])

  PropertyConfigurator.configure(Propertizer("log4j", config.log4j))

  CharsetFor.registerContentTypeCharset("application/json+cjson", StandardCharsets.UTF_8)

  val logOptions = LoggingOptions(LoggerFactory.getLogger(""),
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
              SodaUtils.handleError(req, e)(resp)
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

  private val cleanup = new mutable.Stack[Closeable]

  private def i[T](thing: => T): T = {
    var done = false
    try {
      val result = thing
      result match {
        case closeable: Closeable => cleanup.push(closeable)
        case dataSource: DataSource => cleanup.push(new Closeable {
          def close() { DataSources.destroy(dataSource) } // this is a no-op if the data source is not a c3p0 data source
        })
        case _ => // ok
      }
      done = true
      result
    } finally {
      if(!done) close()
    }
  }

  private type Startable = { def start(): Unit }
  private def si[T <: Closeable with Startable](thing: => T): T = {
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

  val livenessChecker = si(new InetLivenessChecker(config.network.httpclient.liveness.interval, config.network.httpclient.liveness.range, config.network.httpclient.liveness.missable, executor, rng, config.network.httpclient.liveness.port))

  val httpClient = i(new HttpClientHttpClient(executor,
    HttpClientHttpClient.defaultOptions.
      withLivenessChecker(livenessChecker).
      withUserAgent("soda fountain")))

  val discoverySnoop = si(new CuratorProviderSnoop(curator,
    config.discovery.serviceBasePath,
    config.dataCoordinatorClient.serviceName))

  val dc: DataCoordinatorClient = i(new CuratedHttpDataCoordinatorClient(
    httpClient,
    discovery,
    discoverySnoop,
    config.dataCoordinatorClient.serviceName,
    config.dataCoordinatorClient.connectTimeout,
    config.dataCoordinatorClient.receiveTimeout))

  val qc: QueryCoordinatorClient = si(new CuratedHttpQueryCoordinatorClient(
    httpClient,
    discovery,
    config.queryCoordinatorClient.serviceName,
    config.queryCoordinatorClient.connectTimeout,
    config.queryCoordinatorClient.receiveTimeout))

  val dataSource = i(DataSourceFromConfig(config.database))

  val store: NameAndSchemaStore = i(new PostgresStoreImpl(dataSource))

  val fbm: FeedbackSecondaryManifestClient = {
    val feedbackSecondaryIdMap: Map[StrategyType, SecondaryId] = config.computationStrategySecondaryId match  {
      case Some(conf) =>
        val map = scala.collection.mutable.Map[StrategyType, SecondaryId]()
        ComputationStrategy.strategies.foreach { case (strategy, _) =>
          if (conf.hasPath(strategy.name)) map.put(strategy, SecondaryId(conf.getString(strategy.name)))
        }
        map.toMap
      case None => Map.empty
    }

    new FeedbackSecondaryManifestClient(dc, feedbackSecondaryIdMap)
  }

  val datasetDAO = i(new DatasetDAOImpl(
    dc, fbm, store, columnSpecUtils,
    () => config.dataCoordinatorClient.instancesForNewDatasets(rng.nextInt(config.dataCoordinatorClient.instancesForNewDatasets.size))))
  val columnDAO = i(new ColumnDAOImpl(dc, fbm, store, columnSpecUtils))
  val rowDAO = i(new RowDAOImpl(store, dc, qc))
  val exportDAO = i(new ExportDAOImpl(store, dc))
  val snapshotDAO = i(new SnapshotDAOImpl(store, dc))

  val metricProvider = i(new NoopMetricProvider) // TODO : Replace with Graphite or rip out metrics code completely

  val etagObfuscator = i(config.etagObfuscationKey.fold(ETagObfuscator.noop) { key => new BlowfishCFBETagObfuscator(key.getBytes("UTF-8")) })

  val tableDropDelay = config.tableDropDelay
  val dataCleanupIntervalSecs = config.dataCleanupInterval.toSeconds
  val router = i {
    import com.socrata.soda.server.resources._

    // TODO: this should probably be a different max size value
    val dataset = Dataset(datasetDAO, config.maxDatumSize)
    val column = DatasetColumn(columnDAO, exportDAO, rowDAO, etagObfuscator, config.maxDatumSize)
    val export = Export(exportDAO, etagObfuscator)
    val resource = Resource(rowDAO, datasetDAO, etagObfuscator, config.maxDatumSize, metricProvider, export, dc)
    val suggest = Suggest(datasetDAO, columnDAO, httpClient, config.suggest)
    val snapshots = Snapshots(snapshotDAO)

    new SodaRouter(
      versionResource = Version.service,
      healthZResource = HealthZ.service,
      datasetColumnResource = column.service,
      datasetColumnPKResource = column.pkservice,
      datasetCreateResource = dataset.createService,
      datasetResource = dataset.service,
      datasetUndeleteResource = dataset.undeleteService,
      resourceResource = resource.service,
      resourceExtensions = resource.extensions,
      resourceRowResource = resource.rowService,
      datasetCopyResource = dataset.copyService,
      datasetSecondaryCopyResource = dataset.secondaryCopyService,
      datasetSecondaryCollocateResource = dataset.secondaryCollocateService,
      datasetSecondaryCollocateJobResource = dataset.secondaryCollocateJobService,
      datasetSecondaryVersionsResource = dataset.secondaryVersionsService,
      datasetVersionResource = dataset.versionService,
      datasetExportResource = export.publishedService,
      datasetExportCopyResource = export.service,
      exportExtensions = export.extensions,
      datasetRollupsResource = dataset.rollupService(_, None),
      datasetRollupResource = { case (resourceName, rollupName) => dataset.rollupService(resourceName, Some(rollupName)) },
      sampleResource = suggest.sampleService,
      suggestResource = suggest.service,
      snapshotResources = SnapshotResources(snapshots.findDatasetsService, snapshots.listSnapshotsService, snapshots.snapshotsService)
    )
  }

  //For each of the datasets, call a delete function on each one of them
  //Remove datasets from truth and secondary and sodafountain
  val finished = new CountDownLatch(1)
  val tableDropper = new Thread() {
    setName("table dropper")

    override def run() {
      while (!finished.await(dataCleanupIntervalSecs + Random.nextInt(dataCleanupIntervalSecs.toInt / 10), TimeUnit.SECONDS)) {
        try {
          val records = store.lookupDroppedDatasets(tableDropDelay)
          records.foreach { rec =>
            log.info(s"Dropping dataset ${rec.resourceName} (${rec.systemId}")
            //drops the dataset and calls data coordinator to remove datasets in truth
            datasetDAO.removeDataset("", rec.resourceName, RequestId.generate())
          }
        }
        catch {
          case e: Exception =>
            log.error("Unexpected error while cleaning tables", e)
        }
      }
    }
  }

  def close() { // simulate a cascade of "finally" blocks
    var pendingException: Throwable = null
    while(cleanup.nonEmpty) {
      try { cleanup.pop().close() }
      catch { case t: Throwable =>
        if(pendingException != null) pendingException.addSuppressed(t)
        else pendingException = t
      }
    }
    if(pendingException != null) throw pendingException
  }
}
