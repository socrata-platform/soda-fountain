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
import com.socrata.datacoordinator.client.DataCoordinatorClient.{VersionReport, RowOpReport, SchemaSpec}


object DataCoordinatorClient {

  object SchemaSpec {
    implicit val codec = SimpleJsonCodecBuilder[SchemaSpec].build("hash", _.hash, "pk", _.pk, "schema", _.schema)
  }
  class SchemaSpec(val hash: String, val pk: String, val schema: Map[String,String]) {
    override def toString = JsonUtil.renderJson(this)
  }

  object RowOpReport {
    implicit val codec = SimpleJsonCodecBuilder[RowOpReport].build("rows_created", _.createdCount , "rows_updated", _.updatedCount, "rows_deleted", _.deletedCount)
  }
  class RowOpReport(val createdCount: Int, val updatedCount: Int, val deletedCount: Int)

  object VersionReport{
    implicit val codec = SimpleJsonCodecBuilder[VersionReport].build("version", _.version)
  }
  class VersionReport(val version: Long)

}

trait DataCoordinatorClient {

  def hostO: Option[String]
  def createUrl(host: String) = url(host) / "dataset"
  def mutateUrl(host: String, datasetId: String) = url(host) / "dataset" / datasetId
  def schemaUrl(host: String, datasetId: String) = url(host) / "dataset" / datasetId / "schema"
  def secondaryUrl(host: String, datasetId: String) = url(host) / "secondary-manifest" / "es" / datasetId

  def withHost[T]( f: (String) => Future[Either[Throwable, T]]) : Future[Either[Throwable, T]] = {
    hostO match {
      case Some(host) => f(host)
      case None => Future(Left(new Exception("could not connect to data coordinator")))
    }
  }

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
    withHost { host =>
      val r = secondaryUrl(host, datasetId).POST
      Http(r).either
    }
  }

  def getSchema(datasetId:String) = {
    withHost { host =>
      val request = schemaUrl(host, datasetId).GET
      for ( response <- Http(request) ) yield {
        val osch = JsonUtil.readJson[SchemaSpec](new StringReader(response.getResponseBody))
        osch match {
          case Some(sch) => Right(sch)
          case None => Left(new Exception("schema not found"))
        }
      }
    }
  }

  def sendMutateRequest(datasetId: String, script: MutationScript) = {
    withHost { host =>
      val request = mutateUrl(host, datasetId).
        POST.
        addHeader("Content-Type", "application/json").
        setBody(jsonWriter(script))
      val response = Http(request).either
      response
    }
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
    withHost { host =>
      val createScript = new MutationScript(user, CreateDataset(locale), instructions.getOrElse(Array().iterator))
      for(response <- sendScript(createUrl(host).POST, createScript)) yield response match {
        case Right(r) => {
          val body = r.getResponseBody
          r.getStatusCode match {
            case 200 => {
              val idAndReports = JsonUtil.readJson[JArray](new StringReader(body))
              idAndReports match {
                case Some(JArray(Seq(JString(datasetId), _*))) => {
                  Right((datasetId, idAndReports.get.tail))
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
  }
  def update(datasetId: String, schema: Option[String], user: String, instructions: Iterator[DataCoordinatorInstruction]) = {
    withHost { host =>
      val updateScript = new MutationScript(user, UpdateDataset(schema), instructions)
      val future = sendScript(mutateUrl(host, datasetId).POST, updateScript)
      for (r <- future) yield r match {
        case Right(response) => Right(response) //JsonUtil.readJson[RowOpReport](new StringReader(response.getResponseBody)) match {
          //case Some(rr) => Right(rr)
          //case None => Left(new Exception("could not read row report"))
        //}
        case Left(thr) => Left(thr)
      }
    }
  }
  def copy(datasetId: String, schema: Option[String], copyData: Boolean, user: String, instructions: Option[Iterator[DataCoordinatorInstruction]]) = {
    withHost { host =>
      val createScript = new MutationScript(user, CopyDataset(copyData, schema), instructions.getOrElse(Array().iterator))
      sendScript(mutateUrl(host, datasetId).POST, createScript)
    }
  }
  def publish(datasetId: String, schema: Option[String], snapshotLimit:Option[Int], user: String, instructions: Option[Iterator[DataCoordinatorInstruction]]) = {
    withHost { host =>
      val pubScript = new MutationScript(user, PublishDataset(snapshotLimit, schema), instructions.getOrElse(Array().iterator))
      sendScript(mutateUrl(host, datasetId).POST, pubScript)
    }
  }
  def dropCopy(datasetId: String, schema: Option[String], user: String, instructions: Option[Iterator[DataCoordinatorInstruction]]) = {
    withHost { host =>
      val dropScript = new MutationScript(user, DropDataset(schema), instructions.getOrElse(Array().iterator))
      sendScript(mutateUrl(host, datasetId).POST, dropScript)
    }
  }
  def deleteAllCopies(datasetId: String, schema: Option[String], user: String) = {
    withHost { host =>
      val deleteScript = new MutationScript(user, DropDataset(schema), Array().iterator)
      sendScript(mutateUrl(host, datasetId).DELETE, deleteScript)
    }
  }

  def checkVersionInSecondary(datasetId: String, secondaryName: String): Future[Either[Throwable, VersionReport]] = {
    withHost { host =>
      val request = secondaryUrl(host, datasetId)
      Http(request).map { r =>
        val oVer = JsonUtil.readJson[VersionReport](new StringReader(r.getResponseBody))
        oVer match {
          case Some(ver) => ver
          case None => throw new Exception("version not found")
        }
      }.either
    }
  }
}
