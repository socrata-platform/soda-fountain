package com.socrata.querycoordinator.client

import dispatch._, Defaults._

trait QueryCoordinatorClient {

  val qchost : String

  def query(datasetId: String, query: String) = {
    val request = url(qchost) <<? Map(("ds" -> datasetId), ("q" -> query))
    Http(request)
  }
}
