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
          resp.reset()
          SodaUtils.internalError(req, e)(resp)
      }
    }
  }

  // Below this line is all setup.
  // Note: all initialization that can possibly throw should
  // either go ABOVE the declaration of "cleanup" or be guarded
  // by i() or si() to ensure things are cleaned up if something
  // goes wrong.

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

  val curator = si(CuratorFrameworkFactory.builder.
    connectString(config.curator.ensemble).
    sessionTimeoutMs(config.curator.sessionTimeout.toMillis.toInt).
    connectionTimeoutMs(config.curator.connectTimeout.toMillis.toInt).
    retryPolicy(new retryPolicies.BoundedExponentialBackoffRetry(config.curator.baseRetryWait.toMillis.toInt,
      config.curator.maxRetryWait.toMillis.toInt,
      config.curator.maxRetries)).
    namespace(config.curator.namespace)
    build())

  val discovery = si(ServiceDiscoveryBuilder.builder(classOf[AuxiliaryData]).
    client(curator).
    basePath(config.curator.serviceBasePath).
    build())

  val datasetDAO = ???
  val columnDAO = ???

  val router = i {
    import com.socrata.soda.server.resources._

    val resource = Resource()
    val dataset = Dataset(datasetDAO, config.maxDatumSize)
    val column = DatasetColumn(columnDAO, config.maxDatumSize)

    new SodaRouter(
      datasetColumnResource = column.service,
      datasetCreateResource = dataset.createService,
      datasetResource = dataset.service,
      resourceResource = resource.service,
      resourceRowResource = resource.rowService,
      versionResource = Version.service
    )
  }

  def close() {
    while(!cleanup.isEmpty) cleanup.pop().close()
  }
}
