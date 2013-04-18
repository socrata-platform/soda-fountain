package com.socrata.datacoordinator.client

import dispatch._, Defaults._

object DataCoordinatorClient {

}

class DataCoordinatorClient(val baseUrl: String) {

  def dcUrl(relativeUrl: String) = host(baseUrl) / relativeUrl

  def sendMutateRequest(script: MutationScript) = {
    val request = dcUrl("mutate").addHeader("Content-Type", "application/json") << script.toString
    val response = Http(request OK as.String).either
    response
  }
}
