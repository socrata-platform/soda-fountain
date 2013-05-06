package com.socrata.datacoordinator.client

import dispatch._, Defaults._
import com.ning.http.client.Request.EntityWriter
import java.io.{InputStreamReader, BufferedReader, Reader, OutputStream}

import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.ning.http.client.{RequestBuilder, Response}
import com.socrata.http.server.responses
import javax.servlet.http.HttpServletResponse
import com.rojoma.json.util.JsonUtil
import com.rojoma.json.ast._

object DataCoordinatorClient {

  def passThroughResponse(response: Response): HttpServletResponse => Unit = {
    responses.Status(response.getStatusCode) ~>  ContentType(response.getContentType) ~> Content(response.getResponseBody)
  }
}

trait DataCoordinatorClient {

  def baseUrl: String

  val createUrl = host(baseUrl) / "dataset"
  def mutateUrl(datasetId: BigDecimal) = host(baseUrl) / "dataset" / datasetId.toString
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

  def sendMutateRequest(datasetId: BigDecimal, script: MutationScript) = {
    val request = mutateUrl(datasetId).
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

  def create(
              resourceName: String,
              user: String,
              instructions: Option[Iterable[DataCoordinatorInstruction]],
              locale: String = "en_US")
    : Either[Throwable, (BigDecimal, String)] =
  {
    val createScript = new MutationScript(user, CreateDataset(locale), instructions.getOrElse(Array().toIterable))
    val response = sendScript(createUrl.POST, createScript)
    response() match {
      case Right(r) => {
        val idAndHash = JsonUtil.readJson[JArray](new InputStreamReader(r.getResponseBodyAsStream))
        idAndHash match {
          case Some(a) => {
            val b = a.toArray
            Right((BigDecimal(b(0).toString()), b(1).toString))
          }
          case None => Left(new Error("unexpected response from data coordinator"))
        }
      }
      case Left(t) => Left(t)
    }
  }
  def update(datasetId: BigDecimal, schema: String, user: String, instructions: Iterable[DataCoordinatorInstruction]) = {
    val updateScript = new MutationScript(user, UpdateDataset(schema), instructions)
    sendScript(mutateUrl(datasetId).POST, updateScript)
  }
  def copy(datasetId: BigDecimal, schema: String, copyData: Boolean, user: String, instructions: Option[Iterable[DataCoordinatorInstruction]]) = {
    val createScript = new MutationScript(user, CopyDataset(copyData, schema), instructions.getOrElse(Array().toIterable))
    sendScript(mutateUrl(datasetId).POST, createScript)
  }
  def publish(datasetId: BigDecimal, schema: String, snapshotLimit:Option[Int], user: String, instructions: Option[Iterable[DataCoordinatorInstruction]]) = {
    val pubScript = new MutationScript(user, PublishDataset(snapshotLimit, schema), instructions.getOrElse(Array().toIterable))
    sendScript(mutateUrl(datasetId).POST, pubScript)
  }
  def dropCopy(datasetId: BigDecimal, schema: String, user: String, instructions: Option[Iterable[DataCoordinatorInstruction]]) = {
    val dropScript = new MutationScript(user, DropDataset(schema), instructions.getOrElse(Array().toIterable))
    sendScript(mutateUrl(datasetId).POST, dropScript)
  }
  def deleteAllCopies(datasetId: BigDecimal, schema: String, user: String) = {
    val deleteScript = new MutationScript(user, DropDataset(schema), Array().toIterable)
    sendScript(mutateUrl(datasetId).DELETE, deleteScript)
  }
}
