package com.socrata.soda.external

import com.rojoma.json.ast.{JNull, JValue}
import com.rojoma.json.io.{JsonReader, JValueEventIterator}
import com.rojoma.simplearm.util._
import com.socrata.http.client._
import com.socrata.http.common.AuxiliaryData
import com.socrata.thirdparty.curator.{CuratorServerProvider, CuratorServiceBase}
import com.socrata.thirdparty.curator.ServerProvider._
import org.apache.curator.x.discovery.ServiceDiscovery
import org.slf4j.LoggerFactory
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

object SodaFountainClient {
  sealed abstract class Result
  case class Failed(exception: Exception) extends Result
  case class Response(responseCode: Int, body: Option[JValue]) extends Result
}

/**
 * Manages connections and requests to the Soda Fountain service
 * @param httpClient HttpClient object used to make requests
 * @param discovery Service discovery object for querying Zookeeper
 * @param serviceName Service name as registered in Zookeeper
 * @param connectTimeout Timeout setting for connecting to the service
 */
class SodaFountainClient(httpClient: HttpClient,
                         discovery: ServiceDiscovery[AuxiliaryData],
                         serviceName: String,
                         connectTimeout: FiniteDuration,
                         retryCount: Int,
                         retryWhen: RetryWhen,
                         onNoServers: => Nothing)
  extends CuratorServiceBase(discovery, serviceName) {
  import SodaFountainClient._


  val logger = LoggerFactory.getLogger(getClass)

  private[this] val connectTimeoutMS = connectTimeout.toMillis.toInt
  if(connectTimeoutMS != connectTimeout.toMillis) {
    throw new IllegalArgumentException("Connect timeout out of range (milliseconds must fit in an int)")
  }

  private def baseRequest(rb: RequestBuilder): RequestBuilder = rb.connectTimeoutMS(connectTimeoutMS)

  private val serverProvider = CuratorServerProvider(httpClient, provider, baseRequest)

  /**
   * Sends a request to Soda Fountain to create a dataset
   * and returns the response
   * @param payload Request POST body
   * @return HTTP response code and body
   */
  def create(payload: JValue): Result = post(createUrl, payload)

  /**
   * Sends a request to Soda Fountain to publish a dataset
   * and returns the response
   * @param resourceName Resource name of the dataset to publish
   * @return HTTP response code and body
   */
  def publish(resourceName: String): Result = post(publishUrl(_, resourceName), JNull)

  /**
   * Sends a request to Soda Fountain to upsert rows to a dataset
   * and returns the response
   * @param resourceName Resource name of the dataset to upsert to
   * @param payload Request POST body
   * @return HTTP response code and body
   */
  def upsert(resourceName: String, payload: JValue): Result = post(upsertUrl(_, resourceName), payload)

  /**
   * Sends a request to Soda Fountain to query or retrieve rows from a dataset
   * @param resourceName Resource name of the dataset to query
   * @param ext MimeType extension indicating the format in which Soda Fountain should return a response
   */
  def query(resourceName: String, ext: Option[String] = None): Result = get(queryUrl(_, resourceName, ext))

  private def createUrl(rb: RequestBuilder) =
    rb.p("dataset").method("POST").addHeader(("Content-Type", "application/json"))

  private def publishUrl(rb: RequestBuilder, resourceName: String) =
    rb.p("dataset-copy", resourceName, "_DEFAULT_").method("POST")

  private def upsertUrl(rb: RequestBuilder, resourceName: String) =
    rb.p("resource", resourceName).method("POST").addHeader(("Content-Type", "application/json"))

  private def queryUrl(rb: RequestBuilder, resourceName: String, ext: Option[String]) = {
    val resource = ext match {
      case Some(str) => resourceName + s".$str"
      case None      => resourceName
    }
    rb.p("resource", resource)
  }

  private def post(requestBuilder: RequestBuilder => RequestBuilder, payload: JValue): Result =
    requestAndGetResponse { rb => requestBuilder(rb).json(JValueEventIterator(payload)) }

  private def get(requestBuilder: RequestBuilder => RequestBuilder): Result =
    requestAndGetResponse { rb => requestBuilder(rb).get }

  private def requestAndGetResponse(request: RequestBuilder => SimpleHttpRequest): Result =
    try {
      serverProvider.withRetries(retryCount, request, retryWhen) {
        case Some(response) =>
          for {reader <- managed(response.asReader())} yield {
            try {
              // TODO : Distinguish between empty response and invalid-JSON response
              // TODO : May need to support non-JSON body (eg. CSV) in the future
              val json = Try(JsonReader.fromReader(reader)).toOption
              Complete(Response(response.resultCode, json))
            }
            catch {
              case e: Exception => Retry(e)
            }
          }
        case None => onNoServers
      }
    }
    catch {
      case e: Exception => Failed(e)
    }
}
