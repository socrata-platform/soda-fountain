package com.socrata.datacoordinator.client

import dispatch._, Defaults._
import com.ning.http.client.Request.EntityWriter
import java.io.OutputStream

object DataCoordinatorClient {

}

class DataCoordinatorClient(val baseUrl: String) {

  protected def dcUrl(relativeUrl: String) = host(baseUrl) / relativeUrl

  protected def jsonWriter(script: MutationScript): EntityWriter = new EntityWriter {
    def writeEntity(out: OutputStream) {
      val sw = new java.io.StringWriter()
      script.streamJson(sw)
      val buffered = sw.toString
      printf("sending: ")
      printf(buffered)
      script.streamJson(out)
    }
  }

  def sendMutateRequest(script: MutationScript) = {
    val request = dcUrl("mutate").
      POST.
      addHeader("Content-Type", "application/json").
      setBody(jsonWriter(script))
    val response = Http(request).either
    response
  }
}
