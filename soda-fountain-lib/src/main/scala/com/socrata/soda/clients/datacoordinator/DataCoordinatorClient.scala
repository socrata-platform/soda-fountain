package com.socrata.soda.clients.datacoordinator

import com.rojoma.json.util._
import com.rojoma.json.codec._
import com.rojoma.json.ast._
import com.socrata.soql.types.SoQLType
import com.socrata.soql.environment._
import scala.collection.Map
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

  object VersionReport{
    implicit val codec = SimpleJsonCodecBuilder[VersionReport].build("version", _.version)
  }
  class VersionReport(val version: Long)


}

trait DataCoordinatorClient {
  import DataCoordinatorClient._

  def propagateToSecondary(datasetId: DatasetId, secondaryId: SecondaryId)
  def getSchema(datasetId: DatasetId): Option[SchemaSpec]

  def create(instance: String,
             user: String,
             instructions: Option[Iterator[DataCoordinatorInstruction]],
             locale: String = "en_US") : (DatasetId, Iterable[JValue])
  def update[T](datasetId: DatasetId, schemaHash: String, user: String, instructions: Iterator[DataCoordinatorInstruction])(f: Iterator[JValue] => T): T
  def copy[T](datasetId: DatasetId, copyData: Boolean, user: String, instructions: Iterator[DataCoordinatorInstruction] = Iterator.empty)(f: Iterator[JValue] => T): T
  def publish[T](datasetId: DatasetId, snapshotLimit:Option[Int], user: String, instructions: Iterator[DataCoordinatorInstruction] = Iterator.empty)(f: Iterator[JValue] => T): T
  def dropCopy[T](datasetId: DatasetId, user: String, instructions: Iterator[DataCoordinatorInstruction] = Iterator.empty)(f: Iterator[JValue] => T): T
  def deleteAllCopies[T](datasetId: DatasetId, schema: Option[String], user: String)(f: Iterator[JValue] => T): T
  def checkVersionInSecondary(datasetId: DatasetId, secondaryId: SecondaryId): VersionReport
}
