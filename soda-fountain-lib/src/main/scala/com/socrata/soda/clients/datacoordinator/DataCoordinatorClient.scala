package com.socrata.soda.clients.datacoordinator

import com.rojoma.json.util._
import com.rojoma.json.codec._
import com.rojoma.json.ast._
import com.socrata.soql.types.SoQLType
import com.socrata.soql.environment._
import scala.collection.Map
import com.socrata.http.client._
import com.socrata.http.server.routing.HttpMethods._
import scala.util._
import com.rojoma.json.ast.JString
import com.socrata.soda.server.id.{SecondaryId, DatasetId}

object DataCoordinatorClient {

  implicit object columnNameCodec extends JsonCodec[ColumnName] {
    def encode(x: ColumnName) = JString(x.toString)
    def decode(x: JValue) = x match {
      case JString(s) => Some(ColumnName(s))
      case _ => None
    }
  }

  class JsonInvalidSchemaException(pair: (String, JValue)) extends Exception

  implicit object anotherMapCodec extends JsonCodec[Map[ColumnName, SoQLType]] {
    def encode(x: Map[ColumnName, SoQLType]) = JObject(x.map(pair => (pair._1.toString, JString(pair._2.name.name))))
    def decode(x: JValue): Option[Map[ColumnName, SoQLType]] = x match {
      case JObject(m) =>
        val map = m.map{ pair =>
          pair._2 match {
            case JString(t) => (ColumnName(pair._1), SoQLType.typesByName.get(TypeName(t)).getOrElse(throw new JsonInvalidSchemaException(pair)))
            case _ => return None
          }
        }
        Some(map)
      case _ => None
    }
  }

  object SchemaSpec {
    implicit val codec = SimpleJsonCodecBuilder[SchemaSpec].build("hash", _.hash, "pk", _.pk, "schema", _.schema)
  }
  class SchemaSpec(val hash: String, val pk: ColumnName, val schema: Map[ColumnName,SoQLType]) {
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
  def hostO: Option[String]
  def createUrl(host: String) = RequestBuilder(host).p("dataset")
  def mutateUrl(host: String, datasetId: DatasetId) = RequestBuilder(host).p("dataset", datasetId.underlying)
  def schemaUrl(host: String, datasetId: DatasetId) = RequestBuilder(host).p("dataset", datasetId.underlying, "schema")
  def secondaryUrl(host: String, secondaryId: SecondaryId, datasetId: DatasetId) = RequestBuilder(host).p("secondary-manifest", secondaryId.underlying, datasetId.underlying)

  def withHost[T]( f: (String) => T) : Try[T] = {
    Try {
      hostO match {
        case Some(host) => f(host)
        case None => throw new Exception("could not connect to data coordinator")
      }
    }
  }

  def propagateToSecondary(datasetId: DatasetId, secondaryId: SecondaryId) = {
    withHost { host =>
      val r = secondaryUrl(host, secondaryId, datasetId).method(POST).get
      for (response <- internalHttpClient.execute(r)) yield {
        response.resultCode match {
          case 200 => Right(Unit)
          case _ => Left(new Exception("could not propagate to secondary"))
        }
      }
    }
  }

  def getSchema(datasetId: DatasetId) = {
    withHost { host =>
      val request = schemaUrl(host, datasetId).get
      for (response <- internalHttpClient.execute(request)) yield {
        response.asValue[SchemaSpec]().getOrElse(throw new Exception("schema not found"))
      }
    }
  }

  protected def sendScript[T]( rb: RequestBuilder, script: MutationScript) (f: ((Response) => T)) : T = {
    val request = rb.method(POST).json(script.it)
    for (r <- internalHttpClient.execute(request)) yield f(r)
  }

  def create(  user: String,
              instructions: Option[Iterator[DataCoordinatorInstruction]],
              locale: String = "en_US") = {
    withHost { host =>
      val createScript = new MutationScript(user, CreateDataset(locale), instructions.getOrElse(Array().iterator))
      sendScript(createUrl(host).method(POST), createScript){ response : Response =>
        val idAndReports = response.asValue[JArray]()
        idAndReports match {
          case Some(JArray(Seq(JString(datasetId), _*))) => (DatasetId(datasetId), idAndReports.get.tail)
          case None => throw new Error("unexpected response from data coordinator")
        }
      }
    }
  }
  def update(datasetId: DatasetId, schema: Option[String], user: String, instructions: Iterator[DataCoordinatorInstruction]) = {
    withHost { host =>
      val updateScript = new MutationScript(user, UpdateDataset(schema), instructions)
      sendScript(mutateUrl(host, datasetId).method(POST), updateScript){ r => r.asArray[RowOpReport]() }
    }
  }
  def copy(datasetId: DatasetId, schema: Option[String], copyData: Boolean, user: String, instructions: Option[Iterator[DataCoordinatorInstruction]]) = {
    withHost { host =>
      val createScript = new MutationScript(user, CopyDataset(copyData, schema), instructions.getOrElse(Array().iterator))
      sendScript(mutateUrl(host, datasetId).method(POST), createScript){ r => r.asArray[RowOpReport]() }
    }
  }
  def publish(datasetId: DatasetId, schema: Option[String], snapshotLimit:Option[Int], user: String, instructions: Option[Iterator[DataCoordinatorInstruction]]) = {
    withHost { host =>
      val pubScript = new MutationScript(user, PublishDataset(snapshotLimit, schema), instructions.getOrElse(Array().iterator))
      sendScript(mutateUrl(host, datasetId).method(POST), pubScript){ r => r.asArray[RowOpReport]() }
    }
  }
  def dropCopy(datasetId: DatasetId, schema: Option[String], user: String, instructions: Option[Iterator[DataCoordinatorInstruction]]) = {
    withHost { host =>
      val dropScript = new MutationScript(user, DropDataset(schema), instructions.getOrElse(Array().iterator))
      sendScript(mutateUrl(host, datasetId).method(POST), dropScript){ r => r.asArray[RowOpReport]() }
    }
  }
  def deleteAllCopies(datasetId: DatasetId, schema: Option[String], user: String) = {
    withHost { host =>
      val deleteScript = new MutationScript(user, DropDataset(schema), Array().iterator)
      sendScript(mutateUrl(host, datasetId).method(DELETE), deleteScript){ r => r.asArray[RowOpReport]() }
    }
  }

  def checkVersionInSecondary(datasetId: DatasetId, secondaryId: SecondaryId): Try[VersionReport] = {
    withHost { host =>
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
