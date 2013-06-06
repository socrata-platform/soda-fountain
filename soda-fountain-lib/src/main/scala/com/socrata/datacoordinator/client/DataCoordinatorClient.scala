package com.socrata.datacoordinator.client

import dispatch._, Defaults._
import com.ning.http.client.Request.EntityWriter
import java.io._

import com.socrata.http.server.responses._
import com.socrata.http.server.implicits._
import com.ning.http.client.{RequestBuilder, Response}
import com.socrata.http.server.responses
import javax.servlet.http.HttpServletResponse
import com.rojoma.json.util.{SimpleJsonCodecBuilder, JsonUtil}
import com.rojoma.json.ast._
import com.socrata.datacoordinator.client.DataCoordinatorClient.{RowOpReport, SchemaSpec}


object DataCoordinatorClient {

  object SchemaSpec {
    implicit val codec = SimpleJsonCodecBuilder[SchemaSpec].build("hash", _.hash, "pk", _.pk, "schema", _.schema)
  }
  class SchemaSpec(val hash: String, val pk: String, val schema: Map[String,String]) {
    override def toString = JsonUtil.renderJson(this)
  }

  class RowOpReport(val createdCount: Int, val updatedCount: Int, val deletedCount: Int)
  object RowOpReport {
    implicit val codec = SimpleJsonCodecBuilder[RowOpReport].build("rows_created", _.createdCount , "rows_updated", _.updatedCount, "rows_deleted", _.deletedCount)
  }

}

trait DataCoordinatorClient {

  def baseUrl: String
  def createUrl = url(baseUrl) / "dataset"
  def mutateUrl(datasetId: String) = url(baseUrl) / "dataset" / datasetId
  def schemaUrl(datasetId: String) = url(baseUrl) / "dataset" / datasetId / "schema"
  def secondaryUrl(datasetId: String) = url(baseUrl) / "secondary-manifest" / "es" / datasetId

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

  def propagateToSecondary(datasetId: String) = {
    val r = secondaryUrl(datasetId).POST
    Http(r).either
  }

  def getSchema(datasetId:String) = {
    val request = schemaUrl(datasetId).GET
    for ( response <- Http(request) ) yield {
      val osch = JsonUtil.readJson[SchemaSpec](new StringReader(response.getResponseBody))
      osch match {
        case Some(sch) => Right(sch)
        case None => Left("schema not found")
      }
    }
  }

  def sendMutateRequest(datasetId: String, script: MutationScript) = {
    val request = mutateUrl(datasetId).
      POST.
      addHeader("Content-Type", "application/json").
      setBody(jsonWriter(script))
    val response = Http(request).either
    response
  }

//  So it seems that Dispatch doesn't support streaming/chunked requests.  I'll have to find (or write) a way to do this.
//  TODO: find a way to do request streaming
//
//  def streamMutateRequest(datasetId: String, script: MutationScript): Future[Either[Throwable, Response]] = {
//  }

  def sendScript( rb: RequestBuilder, script: MutationScript) = {
    rb.addHeader("Content-Type", "application/json").setBody(jsonWriter(script))
    val response = Http(rb).either
    response
  }

  def create(  user: String,
              instructions: Option[Iterator[DataCoordinatorInstruction]],
              locale: String = "en_US") = {
    val createScript = new MutationScript(user, CreateDataset(locale), instructions.getOrElse(Array().iterator))
    for(response <- sendScript(createUrl.POST, createScript)) yield response match {
      case Right(r) => {
        val body = r.getResponseBody
        r.getStatusCode match {
          case 200 => {
            JsonUtil.readJson[JArray](new StringReader(body)) match {
              case Some(JArray(Seq(JString(datasetId), JArray(rowReports)))) => {
                Right((datasetId, rowReports))
              }
              case None => Left(new Error("unexpected response from data coordinator: " + body))
            }
          }
          case _ => Left(new Error(body))
        }
      }
      case Left(t) => Left(t)
    }
  }
  def update(datasetId: String, schema: Option[String], user: String, instructions: Iterator[DataCoordinatorInstruction]) = {
    val updateScript = new MutationScript(user, UpdateDataset(schema), instructions)
    val future = sendScript(mutateUrl(datasetId).POST, updateScript)
    for (r <- future) yield r match {
      case Right(response) => Right(response) //JsonUtil.readJson[RowOpReport](new StringReader(response.getResponseBody)) match {
        //case Some(rr) => Right(rr)
        //case None => Left(new Exception("could not read row report"))
      //}
      case Left(thr) => Left(thr)
    }
  }
  def copy(datasetId: String, schema: Option[String], copyData: Boolean, user: String, instructions: Option[Iterator[DataCoordinatorInstruction]]) = {
    val createScript = new MutationScript(user, CopyDataset(copyData, schema), instructions.getOrElse(Array().iterator))
    sendScript(mutateUrl(datasetId).POST, createScript)
  }
  def publish(datasetId: String, schema: Option[String], snapshotLimit:Option[Int], user: String, instructions: Option[Iterator[DataCoordinatorInstruction]]) = {
    val pubScript = new MutationScript(user, PublishDataset(snapshotLimit, schema), instructions.getOrElse(Array().iterator))
    sendScript(mutateUrl(datasetId).POST, pubScript)
  }
  def dropCopy(datasetId: String, schema: Option[String], user: String, instructions: Option[Iterator[DataCoordinatorInstruction]]) = {
    val dropScript = new MutationScript(user, DropDataset(schema), instructions.getOrElse(Array().iterator))
    sendScript(mutateUrl(datasetId).POST, dropScript)
  }
  def deleteAllCopies(datasetId: String, schema: Option[String], user: String) = {
    val deleteScript = new MutationScript(user, DropDataset(schema), Array().iterator)
    sendScript(mutateUrl(datasetId).DELETE, deleteScript)
  }


}
