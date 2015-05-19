package com.socrata.soda.clients.regioncoder

import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder
import com.socrata.soda.clients.regioncoder.RegionCoderClient._
import com.socrata.soda.server.config.RegionCoderClientConfig
import com.socrata.thirdparty.curator.CuratorServiceBase
import org.apache.curator.x.discovery.ServiceDiscovery
import scalaj.http.{HttpOptions, Http}
import java.net.ConnectException

object RegionCoderClient {
  // When passing on the region-coder error response,
  // we'll truncate it to this number of characters.
  val PartialResponseLength = 500

  sealed trait VersionCheckResult
  case object Success extends VersionCheckResult
  case class Failure(url: String, status: Int, partialResponse: String) extends VersionCheckResult
  object Failure {
    implicit val jCodec = AutomaticJsonCodecBuilder[Failure]
  }
}

trait RegionCoderClient {
  def versionCheck: VersionCheckResult
}

case class CuratedRegionCoderClient[T](discovery: ServiceDiscovery[T], config: RegionCoderClientConfig)
  extends CuratorServiceBase(discovery, config.serviceName) with RegionCoderClient {

  private def urlPrefix: Option[String] = Option(provider.getInstance()).map { serv => serv.buildUriSpec() }

  private def request(url: String): Http.Request = Http.get(url)
    .option(HttpOptions.connTimeout(config.connectTimeout.toMillis.toInt))
    .option(HttpOptions.readTimeout(config.readTimeout.toMillis.toInt))

  def versionCheck = {
    urlPrefix match {
      case Some(prefix) =>
        val url = prefix + "version"
        try {
          val (status, _, response) = request(url).asHeadersAndParse(Http.readString)
          if (status == 200 && response.contains("version")) {
            Success
          } else {
            Failure(url, status, response.take(PartialResponseLength))
          }
        } catch {
          case e: ConnectException =>
            // Occurs when region-coder is in the middle of shutting down
            Failure(url, 0, e.getMessage.take(PartialResponseLength))
          case e: scalaj.http.HttpException =>
            Failure(url, e.code, e.body.take(PartialResponseLength))
        }
      case None =>
        Failure("", 0, "Unable to get region-coder instance from Curator")
    }
  }
}
