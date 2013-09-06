package com.socrata.soda.clients.datacoordinator

import com.rojoma.json.util._
import com.rojoma.json.codec._
import com.rojoma.json.ast._
import com.socrata.soql.types.SoQLType
import com.socrata.soql.environment._
import scala.collection.Map
import com.socrata.http.client._
import com.socrata.http.server.routing.HttpMethods._
import com.rojoma.json.ast.JString
import com.socrata.soda.server.id.{ColumnId, SecondaryId, DatasetId}

object DataCoordinatorClient {

  class JsonInvalidSchemaException(pair: (String, JValue)) extends Exception

  implicit object anotherMapCodec extends JsonCodec[Map[ColumnId, SoQLType]] {
    def encode(x: Map[ColumnId, SoQLType]) = JObject(x.map(pair => (pair._1.underlying, JString(pair._2.name.name))))
    def decode(x: JValue): Option[Map[ColumnId, SoQLType]] = x match {
      case JObject(m) =>
        val map = m.map{ pair =>
          pair._2 match {
            case JString(t) => (new ColumnId(pair._1), SoQLType.typesByName.get(TypeName(t)).getOrElse(throw new JsonInvalidSchemaException(pair)))
            case _ => return None
          }
        }
        Some(map)
      case _ => None
    }
  }

  object SchemaSpec {
    implicit val codec = SimpleJsonCodecBuilder[SchemaSpec].build("hash", _.hash, "pk", _.pk, "schema", _.schema, "locale", _.locale)
  }
  class SchemaSpec(val hash: String, val pk: ColumnId, val schema: Map[ColumnId,SoQLType], val locale: String) {
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
  import DataCoordinatorClient._

  val internalHttpClient : HttpClient
  def hostO(instance: String): Option[RequestBuilder]
  def createUrl(host: RequestBuilder) = host.p("dataset")
  def mutateUrl(host: RequestBuilder, datasetId: DatasetId) = host.p("dataset", datasetId.underlying)
  def schemaUrl(host: RequestBuilder, datasetId: DatasetId) = host.p("dataset", datasetId.underlying, "schema")
  def secondaryUrl(host: RequestBuilder, secondaryId: SecondaryId, datasetId: DatasetId) = host.p("secondary-manifest", secondaryId.underlying, datasetId.underlying)

  def withHost[T](instance: String)(f: RequestBuilder => T): T =
    hostO(instance) match {
      case Some(host) => f(host)
      case None => throw new Exception("could not connect to data coordinator")
    }

  def withHost[T](datasetId: DatasetId)(f: RequestBuilder => T): T =
    withHost(datasetId.nativeDataCoordinator)(f)

    def propagateToSecondary(datasetId: DatasetId, secondaryId: SecondaryId): Unit =
    withHost(datasetId) { host =>
      val r = secondaryUrl(host, secondaryId, datasetId).method(POST).get
      for (response <- internalHttpClient.execute(r)) yield {
        response.resultCode match {
          case 200 => // ok
          case _ => throw new Exception("could not propagate to secondary")
        }
      }
    }

  def getSchema(datasetId: DatasetId): Option[SchemaSpec] =
    withHost(datasetId) { host =>
      val request = schemaUrl(host, datasetId).get
      for (response <- internalHttpClient.execute(request)) yield {
        if(response.resultCode == 200) {
          val result = response.asValue[SchemaSpec]()
          if(!result.isDefined) throw new Exception("Unable to interpret response as a schemaspec?")
          result
        } else if(response.resultCode == 404) {
          None
        } else {
          throw new Exception("Unexpected result from server: " + response.resultCode)
        }
      }
    }

  protected def sendScript[T]( rb: RequestBuilder, script: MutationScript) (f: ((Response) => T)): T = {
    val request = rb.method(POST).json(script.it)
    for (r <- internalHttpClient.execute(request)) yield f(r)
  }

  def create(instance: String,
             user: String,
             instructions: Option[Iterator[DataCoordinatorInstruction]],
             locale: String = "en_US") : (DatasetId, Iterable[JValue]) = {
    withHost(instance) { host =>
      val createScript = new MutationScript(user, CreateDataset(locale), instructions.getOrElse(Array().iterator))
      sendScript(createUrl(host).method(POST), createScript){ response : Response =>
        val idAndReports = response.asValue[JArray]()
        idAndReports match {
          case Some(JArray(Seq(JString(datasetId), _*))) => (DatasetId(datasetId), idAndReports.get.tail)
          case None => throw new Exception("unexpected response from data coordinator")
        }
      }
    }
  }

  def update(datasetId: DatasetId, schema: Option[String], user: String, instructions: Iterator[DataCoordinatorInstruction]): Vector[RowOpReport] = {
    withHost(datasetId) { host =>
      val updateScript = new MutationScript(user, UpdateDataset(schema), instructions)
      sendScript(mutateUrl(host, datasetId).method(POST), updateScript){ r => r.asArray[RowOpReport]().toVector }
    }
  }

  def copy(datasetId: DatasetId, schema: Option[String], copyData: Boolean, user: String, instructions: Option[Iterator[DataCoordinatorInstruction]]) = {
    withHost(datasetId) { host =>
      val createScript = new MutationScript(user, CopyDataset(copyData, schema), instructions.getOrElse(Array().iterator))
      sendScript(mutateUrl(host, datasetId).method(POST), createScript){ r => r.asArray[RowOpReport]() }
    }
  }
  def publish(datasetId: DatasetId, schema: Option[String], snapshotLimit:Option[Int], user: String, instructions: Option[Iterator[DataCoordinatorInstruction]]) = {
    withHost(datasetId) { host =>
      val pubScript = new MutationScript(user, PublishDataset(snapshotLimit, schema), instructions.getOrElse(Array().iterator))
      sendScript(mutateUrl(host, datasetId).method(POST), pubScript){ r => r.asArray[RowOpReport]() }
    }
  }
  def dropCopy(datasetId: DatasetId, schema: Option[String], user: String, instructions: Option[Iterator[DataCoordinatorInstruction]]) = {
    withHost(datasetId) { host =>
      val dropScript = new MutationScript(user, DropDataset(schema), instructions.getOrElse(Array().iterator))
      sendScript(mutateUrl(host, datasetId).method(POST), dropScript){ r => r.asArray[RowOpReport]() }
    }
  }
  def deleteAllCopies(datasetId: DatasetId, schema: Option[String], user: String) = {
    withHost(datasetId) { host =>
      val deleteScript = new MutationScript(user, DropDataset(schema), Array().iterator)
      sendScript(mutateUrl(host, datasetId).method(DELETE), deleteScript){ r => r.asArray[RowOpReport]() }
    }
  }

  def checkVersionInSecondary(datasetId: DatasetId, secondaryId: SecondaryId): VersionReport = {
    withHost(datasetId) { host =>
      val request = secondaryUrl(host, secondaryId, datasetId).get
      for (r <- internalHttpClient.execute(request)) yield {
        val oVer = r.asValue[VersionReport]()
        oVer match {
          case Some(ver) => ver
          case None => throw new Exception("version not found")
        }
      }
    }
  }
}
