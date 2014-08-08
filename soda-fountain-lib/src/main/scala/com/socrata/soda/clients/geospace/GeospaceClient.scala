package com.socrata.soda.clients.geospace

import com.socrata.thirdparty.curator.CuratorServiceBase
import org.apache.curator.x.discovery.ServiceDiscovery
import scala.util.Try
import scalaj.http.Http

trait GeospaceClient {
  def versionCheck: Boolean
}

case class CuratedGeospaceClient[T](discovery: ServiceDiscovery[T], serviceName: String) extends CuratorServiceBase(discovery, serviceName) with GeospaceClient {
  def urlPrefix = Option(provider.getInstance()).map { serv => serv.buildUriSpec() }

  def versionCheck = {
    urlPrefix match {
      case Some(prefix) =>
        val url = prefix + "version"
        Try(Http.get(url).asHeadersAndParse(Http.readString)).toOption match {
          case Some((status, _, body)) => status == 200 && body.contains("version")
          case None => false
        }
      case None => false
    }
  }
}