package com.socrata.soda.server

import java.io.Closeable
import scala.collection.mutable
import com.socrata.soda.server.config.SodaFountainConfig
import com.netflix.curator.framework.CuratorFrameworkFactory
import com.netflix.curator.{retry => retryPolicies}
import com.netflix.curator.x.discovery.ServiceDiscoveryBuilder
import com.socrata.http.common.AuxiliaryData
import org.apache.log4j.PropertyConfigurator
import com.socrata.thirdparty.typesafeconfig.Propertizer
import javax.servlet.http.HttpServletRequest
import com.socrata.http.server.HttpResponse
import com.socrata.soda.server.highlevel.{RowDAOImpl, ColumnSpecUtils, DatasetDAOImpl}
import java.security.SecureRandom
import com.socrata.soda.clients.datacoordinator.{CuratedHttpDataCoordinatorClient, DataCoordinatorClient}
import com.socrata.http.client.{InetLivenessChecker, HttpClientHttpClient}
import java.util.concurrent.Executors
import com.socrata.soda.server.util.CloseableExecutorService
import com.socrata.soda.server.persistence.{DataSourceFromConfig, PostgresStoreImpl, NameAndSchemaStore}
import com.socrata.soda.clients.querycoordinator.{CuratedHttpQueryCoordinatorClient, QueryCoordinatorClient}
import scala.concurrent.duration.FiniteDuration

/**
 * Manages the lifecycle of the routing table.  This means that
 * it intializes resources that are necessary across the lifetime
 * of the server for the use of services, knows the routing table,
 * and cleans up the resources on shutdown.
 */
class SodaFountain(config: SodaFountainConfig) extends Closeable {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[SodaFountain])

  PropertyConfigurator.configure(Propertizer("log4j", config.log4j))

  def handle(req: HttpServletRequest): HttpResponse = {
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
      if(result.isInstanceOf[Closeable]) cleanup.push(result.asInstanceOf[Closeable])
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

  val curator = si {
    def ms(value: String, d: FiniteDuration) = {
      val m = d.toMillis.toInt
      if(m != d.toMillis) throw new IllegalArgumentException(value + " out of range (milliseconds must fit in an int)")
      m
    }
    CuratorFrameworkFactory.builder.
      connectString(config.curator.ensemble).
      sessionTimeoutMs(ms("Session timeout", config.curator.sessionTimeout)).
      connectionTimeoutMs(ms("Connect timeout", config.curator.connectTimeout)).
      retryPolicy(new retryPolicies.BoundedExponentialBackoffRetry(ms("Base retry wait", config.curator.baseRetryWait),
        ms("Max retry wait", config.curator.maxRetryWait),
        config.curator.maxRetries)).
      namespace(config.curator.namespace).
      build()
  }

  val discovery = si(ServiceDiscoveryBuilder.builder(classOf[AuxiliaryData]).
    client(curator).
    basePath(config.curator.serviceBasePath).
    build())

  val executor = i(new CloseableExecutorService(Executors.newCachedThreadPool()))

  val livenessChecker = si(new InetLivenessChecker(config.network.httpclient.liveness.interval, config.network.httpclient.liveness.range, config.network.httpclient.liveness.missable, executor, rng))

  val httpClient = i(new HttpClientHttpClient(livenessChecker, executor, userAgent = "soda fountain"))

  val dc: DataCoordinatorClient = i(new CuratedHttpDataCoordinatorClient(httpClient, discovery, config.dataCoordinatorClient.serviceName, config.dataCoordinatorClient.instance, config.dataCoordinatorClient.connectTimeout))

  val qc: QueryCoordinatorClient = si(new CuratedHttpQueryCoordinatorClient(httpClient, discovery, config.queryCoordinatorClient.serviceName, config.queryCoordinatorClient.connectTimeout))

  val dataSource = i(DataSourceFromConfig(config.database))

  val store: NameAndSchemaStore = i(new PostgresStoreImpl(dataSource))

  val datasetDAO = i(new DatasetDAOImpl(dc, store, columnSpecUtils, () => config.dataCoordinatorClient.instance))
  val columnDAO = null
  val rowDAO = i(new RowDAOImpl(store, dc, qc))

  val router = i {
    import com.socrata.soda.server.resources._

    val resource = Resource(rowDAO, config.maxDatumSize) // TODO: this should probably be a different max size value
    val dataset = Dataset(datasetDAO, config.maxDatumSize)
    val column = DatasetColumn(columnDAO, config.maxDatumSize)

    new SodaRouter(
      datasetColumnResource = column.service,
      datasetCreateResource = dataset.createService,
      datasetResource = dataset.service,
      resourceResource = resource.service,
      resourceRowResource = resource.rowService,
      datasetCopyResource = dataset.copyService,
      versionResource = Version.service
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
