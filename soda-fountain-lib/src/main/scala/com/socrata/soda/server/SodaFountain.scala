package com.socrata.soda.server

import com.mchange.v2.c3p0.DataSources
import com.socrata.http.client.{InetLivenessChecker, HttpClientHttpClient}
import com.socrata.http.common.AuxiliaryData
import com.socrata.http.server.util.handlers.{LoggingHandler, ThreadRenamingHandler}
import com.socrata.soda.clients.datacoordinator.{CuratedHttpDataCoordinatorClient, DataCoordinatorClient}
import com.socrata.soda.clients.querycoordinator.{CuratedHttpQueryCoordinatorClient, QueryCoordinatorClient}
import com.socrata.soda.server.computation.ComputedColumns
import com.socrata.soda.server.config.SodaFountainConfig
import com.socrata.soda.server.highlevel._
import com.socrata.soda.server.persistence.pg.PostgresStoreImpl
import com.socrata.soda.server.persistence.{DataSourceFromConfig, NameAndSchemaStore}
import com.socrata.soda.server.metrics.{BalboaMetricProvider, NoopMetricProvider}
import com.socrata.soda.server.util._
import com.socrata.thirdparty.curator.{CuratorFromConfig, DiscoveryFromConfig}
import com.socrata.thirdparty.typesafeconfig.Propertizer
import java.io.Closeable
import java.security.SecureRandom
import java.util.concurrent.Executors
import javax.sql.DataSource
import org.apache.log4j.PropertyConfigurator
import scala.collection.mutable

/**
 * Manages the lifecycle of the routing table.  This means that
 * it intializes resources that are necessary across the lifetime
 * of the server for the use of services, knows the routing table,
 * and cleans up the resources on shutdown.
 */
class SodaFountain(config: SodaFountainConfig) extends Closeable {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[SodaFountain])

  PropertyConfigurator.configure(Propertizer("log4j", config.log4j))

  val handle =
    ThreadRenamingHandler {
      LoggingHandler { req =>
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
              if(!resp.isCommitted) {
                resp.reset()
                SodaUtils.internalError(req, e)(resp)
              } else {
                log.warn("Caught exception but the response is already committed; just cutting the client off", e)
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

  val livenessChecker = si(new InetLivenessChecker(config.network.httpclient.liveness.interval, config.network.httpclient.liveness.range, config.network.httpclient.liveness.missable, executor, rng))

  val httpClient = i(new HttpClientHttpClient(livenessChecker, executor, userAgent = "soda fountain"))

  val dc: DataCoordinatorClient = i(new CuratedHttpDataCoordinatorClient(httpClient, discovery, config.dataCoordinatorClient.serviceName, config.dataCoordinatorClient.instance, config.dataCoordinatorClient.connectTimeout))

  val qc: QueryCoordinatorClient = si(new CuratedHttpQueryCoordinatorClient(httpClient, discovery, config.queryCoordinatorClient.serviceName, config.queryCoordinatorClient.connectTimeout))

  val dataSource = i(DataSourceFromConfig(config.database))

  val store: NameAndSchemaStore = i(new PostgresStoreImpl(dataSource))

  val computedColumns = new ComputedColumns(config.handlers, discovery)

  val datasetDAO = i(new DatasetDAOImpl(dc, store, columnSpecUtils, () => config.dataCoordinatorClient.instance))
  val columnDAO = i(new ColumnDAOImpl(dc, store, columnSpecUtils))
  val rowDAO = i(new RowDAOImpl(store, dc, qc))
  val exportDAO = i(new ExportDAOImpl(store, dc))

  val metricProvider = config.metrics.map( balboaConfig => si(new BalboaMetricProvider(balboaConfig)) ).getOrElse( i(new NoopMetricProvider) )

  val etagObfuscator = i(config.etagObfuscationKey.fold(ETagObfuscator.noop) { key => new BlowfishCFBETagObfuscator(key.getBytes("UTF-8")) })

  val router = i {
    import com.socrata.soda.server.resources._

    // TODO: this should probably be a different max size value
    val resource = Resource(rowDAO, store, etagObfuscator, config.maxDatumSize, computedColumns, metricProvider)
    val dataset = Dataset(datasetDAO, config.maxDatumSize)
    val column = DatasetColumn(columnDAO, etagObfuscator, config.maxDatumSize)
    val export = Export(exportDAO, etagObfuscator)
    val compute = Compute(store, exportDAO, rowDAO, computedColumns, etagObfuscator)

    new SodaRouter(
      versionResource = Version.service,
      healthZResource = HealthZ.service,
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
      computeResource = compute.service
    )
  }

  def close() { // simulate a cascade of "finally" blocks
    var pendingException: Throwable = null
    while(!cleanup.isEmpty) {
      try { cleanup.pop().close() }
      catch { case t: Throwable =>
        if(pendingException != null) pendingException.addSuppressed(t)
        else pendingException = t
      }
    }
    if(pendingException != null) throw pendingException
  }
}
