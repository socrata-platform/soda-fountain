package com.socrata.soda.server.highlevel

import com.socrata.soda.server.id.{DatasetId, ColumnId, ResourceName}
import com.socrata.soda.server.highlevel.RowDAO._
import com.socrata.soda.server.persistence.NameAndSchemaStore
import com.socrata.soda.clients.querycoordinator.QueryCoordinatorClient
import com.socrata.soda.clients.datacoordinator._
import com.rojoma.json.ast.{JBoolean, JObject, JValue}
import com.socrata.soql.environment.ColumnName
import com.socrata.soda.clients.datacoordinator.DataCoordinatorClient.SchemaSpec
import com.socrata.soql.types.SoQLType
import com.socrata.soda.clients.datacoordinator.UpsertRow
import com.socrata.soda.server.id.DatasetId
import com.socrata.soda.server.highlevel.RowDAO.NotFound
import com.socrata.soda.server.highlevel.RowDAO.Success
import com.socrata.soda.clients.datacoordinator.RowUpdateOptionChange

class RowDAOImpl(store: NameAndSchemaStore, dc: DataCoordinatorClient, qc: QueryCoordinatorClient) extends RowDAO {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[RowDAOImpl])
  def user = {
    log.info("Actually get user info from somewhere")
    "soda-fountain"
  }

  def query(resourceName: ResourceName, query: String): Result = {
    store.translateResourceName(resourceName) match {
      case Some((datasetId, schemaLite)) =>
        val (code, response) = qc.query(datasetId, query, schemaLite)
        Success(code, response) // TODO: Gah I don't even know where to BEGIN listing the things that need doing here!
      case None =>
        NotFound(resourceName)
    }
  }

  val LegacyDeleteFlag = new ColumnName(":deleted")

  private case class MaltypedDataEx(col: ColumnName, expected: SoQLType, got: JValue) extends Exception
  private case class UnknownColumnEx(col: ColumnName) extends Exception
  private case class DeleteNoPKEx() extends Exception
  private case class NotAnObjectEx(obj: JValue) extends Exception

  class RowDataTranslator(datasetId: DatasetId, columns: Map[ColumnName, ColumnId], schema: SchemaSpec, ignoreUnknownColumns: Boolean) {
    private[this] sealed abstract class ColumnResult {
      val columnName: ColumnName
    }
    private[this] case class NoColumn(columnName: ColumnName) extends ColumnResult
    private[this] case class ColumnInfo(columnName: ColumnName, id: ColumnId, typ: SoQLType) extends ColumnResult

    // A cache from the keys of the JSON objects which are rows to values
    // which represent either the fact that the key does not represent
    // a known column or the column's ID and type.
    private[this] val columnInfos = new java.util.HashMap[String, ColumnResult]
    private[this] def ciFor(rawColumnName: String): ColumnResult = columnInfos.get(rawColumnName) match {
      case null =>
        val cn = ColumnName(rawColumnName)
        columns.get(cn) match {
          case Some(cid) =>
            if(columnInfos.size > columns.size * 10)
              columnInfos.clear() // bad user, but I'd rather spend CPU than memory
            schema.schema.get(cid) match {
              case Some(typ) =>
                val ci = ColumnInfo(cn, cid, typ)
                columnInfos.put(rawColumnName, ci)
                ci
              case None => // TODO: INCONSISTENCY ALERT
                val nc = NoColumn(cn)
                columnInfos.put(rawColumnName, nc)
                nc
            }
          case None =>
            val nc = NoColumn(cn)
            columnInfos.put(rawColumnName, nc)
            nc
        }
      case r =>
        r
    }

    def convert(row: JValue): RowUpdate = row match {
      case JObject(map) =>
        var rowHasLegacyDeleteFlag = false
        val row: scala.collection.Map[String, JValue] = map.flatMap { case (uKey, uVal) =>
          ciFor(uKey) match {
            case ColumnInfo(colName, cid, typ) =>
              TypeChecker.check(typ, uVal) match {
                case Right(v) => (cid.underlying -> uVal) :: Nil
                case Left(TypeChecker.Error(expected, got)) => throw MaltypedDataEx(colName, expected, got)
              }
            case NoColumn(colName) =>
              if(colName == LegacyDeleteFlag && JBoolean.canonicalTrue == uVal) {
                rowHasLegacyDeleteFlag = true
                Nil
              } else if(ignoreUnknownColumns) {
                Nil
              } else {
                throw UnknownColumnEx(colName)
              }
          }
        }
        if(rowHasLegacyDeleteFlag) {
          row.get(schema.pk.underlying) match {
            case Some(pkVal) => DeleteRow(pkVal)
            case None => throw DeleteNoPKEx()
          }
        } else {
          UpsertRow(row)
        }
      case other =>
        throw NotAnObjectEx(other)
    }
  }

  def doUpsertish[T](resourceName: ResourceName, data: Iterator[JValue], instructions: Iterator[DataCoordinatorInstruction], f: UpsertResult => T): T = {
    store.translateResourceName(resourceName) match {
      case Some((datasetId, columns)) =>
        dc.getSchema(datasetId) match {
          case Some(schema) =>
            val trans = new RowDataTranslator(datasetId, columns, schema, ignoreUnknownColumns = false)
            val upserts = data.map(trans.convert)
            try {
              dc.update(datasetId, schema.hash, user, instructions ++ upserts) { result =>
                f(StreamSuccess(result))
              }
            } catch {
              case UnknownColumnEx(col) =>
                f(UnknownColumn(col))
              case DeleteNoPKEx() =>
                f(DeleteWithoutPrimaryKey)
              case NotAnObjectEx(v) =>
                f(RowNotAnObject(v))
              case MaltypedDataEx(cn, expected, got) =>
                f(MaltypedData(cn, expected, got))
            }
          case None =>
            ??? // TODO: More precise internal error due to inconsitency
        }
      case None =>
        f(NotFound(resourceName))
    }
  }

  def upsert[T](resourceName: ResourceName, data: Iterator[JValue])(f: UpsertResult => T): T =
    doUpsertish(resourceName, data, Iterator.empty, f)

  def replace[T](resourceName: ResourceName, data: Iterator[JValue])(f: UpsertResult => T): T =
    doUpsertish(resourceName, data, Iterator.single(RowUpdateOptionChange(truncate = true)), f)
}
