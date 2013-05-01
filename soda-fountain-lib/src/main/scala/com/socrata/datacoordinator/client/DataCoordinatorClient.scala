package com.socrata.datacoordinator.client

import dispatch._, Defaults._
import com.ning.http.client.Request.EntityWriter
import java.io.OutputStream

import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.ning.http.client.Response
import com.socrata.http.server.responses
import javax.servlet.http.HttpServletResponse
import com.socrata.soda.server.services.SodaService

object DataCoordinatorClient {

  def passThroughResponse(response: Response): HttpServletResponse => Unit = {
    responses.Status(response.getStatusCode) ~>  ContentType(response.getContentType) ~> Content(response.getResponseBody)
  }
}

trait DataCoordinatorClient {

  def baseUrl: String

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

  def sendMutateRequest(datasetResourceName: String, script: MutationScript) = {
    val request = dcUrl("mutate").
      POST.
      addHeader("Content-Type", "application/json").
      setBody(jsonWriter(script))
    val response = Http(request).either
    response
  }
}
