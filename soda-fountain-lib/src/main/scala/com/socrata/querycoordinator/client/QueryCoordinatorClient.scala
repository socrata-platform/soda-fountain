package com.socrata.querycoordinator.client

import dispatch._, Defaults._
import com.ning.http.client.Response

trait QueryCoordinatorClient {

  def qchost : Option[String]

  def query(datasetId: String, query: String): Future[Either[Throwable, Response]] = {
    qchost match {
      case Some(host) =>
        val request = url(host) <<? Map(("ds" -> datasetId), ("q" -> query))
        Http(request).either
      case None => Future {
        Left(new Exception("could not connect to query coordinator"))
      }
    }
  }
}
