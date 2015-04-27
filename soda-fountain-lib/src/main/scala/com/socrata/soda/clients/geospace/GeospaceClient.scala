package com.socrata.soda.clients.geospace

import com.socrata.soda.clients.geospace.GeospaceClient._
import com.socrata.soda.server.config.GeospaceClientConfig
import com.socrata.thirdparty.curator.CuratorServiceBase
import org.apache.curator.x.discovery.ServiceDiscovery
import scalaj.http.{HttpOptions, Http}
import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder

object GeospaceClient {
  val PartialResponseLength = 500

  sealed trait VersionCheckResult
  case object Success extends VersionCheckResult
  case class Failure(url: String, status: Int, partialResponse: String) extends VersionCheckResult
  object Failure {
    implicit val jCodec = AutomaticJsonCodecBuilder[Failure]
  }
}

trait GeospaceClient {
  def versionCheck: VersionCheckResult
}

case class CuratedGeospaceClient[T](discovery: ServiceDiscovery[T], config: GeospaceClientConfig)
  extends CuratorServiceBase(discovery, config.serviceName) with GeospaceClient {

  def urlPrefix: Option[String] = Option(provider.getInstance()).map { serv => serv.buildUriSpec() }

  def request(url: String): Http.Request = Http.get(url)
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
          case e: scalaj.http.HttpException =>
            Failure(url, e.code, e.body.take(PartialResponseLength))
        }
      case None =>
        Failure("", 0, "Unable to get geospace instance from Curator")
    }
  }
}