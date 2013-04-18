package com.socrata.datacoordinator.client

import dispatch._, Defaults._

object DataCoordinatorClient {

}

class DataCoordinatorClient(val baseUrl: String) {

  def mutate(script: MutationScript){
    val request = url("http://" + baseUrl + "/mutate")
    request.addHeader("Content-Type", "application/json")
    request << script.toString
    val response = Http(request)
    //for (r <- response) println("response from DC:" + r)
    response( )
  }
}
