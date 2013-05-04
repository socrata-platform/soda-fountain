package com.socrata.datacoordinator.client

import dispatch._, Defaults._
import com.ning.http.client.Request.EntityWriter
import java.io.OutputStream

import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.ning.http.client.{RequestBuilder, Response}
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

  val createUrl = host(baseUrl) / "dataset"
  def mutateUrl(datasetName: String) = host(baseUrl) / "dataset" / datasetName
  def schemaUrl(datasetName: String) = host(baseUrl) / "dataset" / datasetName / "schema"

  protected def jsonWriter(script: MutationScript): EntityWriter = new EntityWriter {
    def writeEntity(out: OutputStream) {
//      val sw = new java.io.StringWriter()
//      script.streamJson(sw)
//      val buffered = sw.toString
//      printf("sending: ")
//      printf(buffered)
      script.streamJson(out)
    }
  }

  protected def getSchema(resourceName:String) = {
    val request = schemaUrl(resourceName).GET
    Http(request)
  }

  def sendMutateRequest(datasetResourceName: String, script: MutationScript) = {
    val request = mutateUrl(datasetResourceName).
      POST.
      addHeader("Content-Type", "application/json").
      setBody(jsonWriter(script))
    val response = Http(request).either
    response
  }

  def sendScript( rb: RequestBuilder, script: MutationScript) = {
    rb.addHeader("Content-Type", "application/json").setBody(jsonWriter(script))
    val response = Http(rb).either
    response
  }

  def create(resourceName: String, locale: String = "en_US", user: String, instructions: Option[Iterable[DataCoordinatorInstruction]]) = {
    val createScript = new MutationScript(user, CreateDataset(locale), instructions.getOrElse(Array().toIterable))
    sendScript(createUrl.POST, createScript)
  }
  def update(resourceName: String, schema: String, user: String, instructions: Iterable[DataCoordinatorInstruction]) = {
    val updateScript = new MutationScript(user, UpdateDataset(schema), instructions)
    sendScript(mutateUrl(resourceName).POST, updateScript)
  }
  def copy(resourceName: String, schema: String, copyData: Boolean, user: String, instructions: Option[Iterable[DataCoordinatorInstruction]]) = {
    val createScript = new MutationScript(user, CopyDataset(copyData, schema), instructions.getOrElse(Array().toIterable))
    sendScript(mutateUrl(resourceName).POST, createScript)
  }
  def publish(resourceName: String, schema: String, snapshotLimit:Option[Int], user: String, instructions: Option[Iterable[DataCoordinatorInstruction]]) = {
    val pubScript = new MutationScript(user, PublishDataset(snapshotLimit, schema), instructions.getOrElse(Array().toIterable))
    sendScript(mutateUrl(resourceName).POST, pubScript)
  }
  def dropCopy(resourceName: String, schema: String, user: String, instructions: Option[Iterable[DataCoordinatorInstruction]]) = {
    val dropScript = new MutationScript(user, DropDataset(schema), instructions.getOrElse(Array().toIterable))
    sendScript(mutateUrl(resourceName).POST, dropScript)
  }
  def deleteAllCopies(resourceName: String, schema: String, user: String) = {
    val deleteScript = new MutationScript(user, DropDataset(schema), Array().toIterable)
    sendScript(mutateUrl(resourceName).DELETE, deleteScript)
  }
}
