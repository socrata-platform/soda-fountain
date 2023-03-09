package com.socrata.soda.server

import com.socrata.http.server.HttpRequest
import com.socrata.soda.server.AccessControlService._
import com.socrata.soda.server.id.ResourceName
import com.socrata.http.client._
import com.socrata.soda.server.util.CloseableExecutorService

import java.util.concurrent.Executors

import scala.io.{Source}

class AccessControlService {

  val httpClient: HttpClient =
    new HttpClientHttpClient(
      new CloseableExecutorService(Executors.newCachedThreadPool()),
      HttpClientHttpClient.defaultOptions.
        withUserAgent("soda-fountain"))

  /**
   *
   * @param request - user request with auth parameters
   * @param resourceName - resource name
   * @return
   *    Resource mapping if access is granted, or None if access is denied
   */
  def listRollupAuth(request: HttpRequest, resourceName: ResourceName): ResourceNameMapping = {
    val body = auth(request, "rollup", List(("viewUid", resourceName.name), ("op", "list")))
    Dataset(new ResourceName(body))
  }

  /**
   * Generic "ask" method to query access control service
   *
   * @param request - user request
   * @param service - service requesting access
   * @param query - service specific parameters
   * @return
   *      freeform text response (to be interpreted by higher-level methods)
   */
  private def auth(request: HttpRequest, service: String, query: List[(String, String)]): String = {

    val headers: Iterable[(String, String)] = request.headerNames.filter(_ != "Accept-Encoding").map(
      {
        case header => (header, request.header(header).getOrElse(""))
      }
    ).toList

    val req = RequestBuilder("localhost")
      .secure(false)
      .port(8080)
      .p("accesscontrol", service, "auth")
      .query(query)
      .headers(headers)
      .get

    val response = httpClient.executeUnmanaged(req)
    Source.fromInputStream(response.inputStream()).mkString("")
  }

}

object AccessControlService {

  sealed trait ResourceNameMapping
  case class Dataset(mappedName: ResourceName) extends ResourceNameMapping
  case class DerivedView(soql: String) extends ResourceNameMapping

}
