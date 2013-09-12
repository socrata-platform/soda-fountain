package com.socrata.soda.server.highlevel

import com.socrata.soda.server.id.ResourceName
import com.socrata.soda.server.highlevel.RowDAO._
import com.socrata.soda.server.persistence.{ColumnRecordLike, DatasetRecordLike, NameAndSchemaStore}
import com.socrata.soda.clients.querycoordinator.QueryCoordinatorClient
import com.socrata.soda.clients.datacoordinator._
import com.rojoma.json.ast.{JBoolean, JObject, JValue}
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.SoQLType
import com.socrata.soda.clients.datacoordinator.UpsertRow
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
      case Some(datasetRecord) =>
        val (code, response) = qc.query(datasetRecord.systemId, query, datasetRecord.columnsByName.mapValues(_.id))
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

  class RowDataTranslator(dataset: DatasetRecordLike, ignoreUnknownColumns: Boolean) {
    private[this] sealed abstract class ColumnResult
    private[this] case class NoColumn(fieldName: ColumnName) extends ColumnResult
    private[this] case class ColumnInfo(columnRecord: ColumnRecordLike) extends ColumnResult

    // A cache from the keys of the JSON objects which are rows to values
    // which represent either the fact that the key does not represent
    // a known column or the column's ID and type.
    private[this] val columns = dataset.columnsByName
    private[this] val columnInfos = new java.util.HashMap[String, ColumnResult]
    private[this] def ciFor(rawColumnName: String): ColumnResult = columnInfos.get(rawColumnName) match {
      case null =>
        val cn = ColumnName(rawColumnName)
        columns.get(cn) match {
          case Some(cr) =>
            if(columnInfos.size > columns.size * 10)
              columnInfos.clear() // bad user, but I'd rather spend CPU than memory
            val ci = ColumnInfo(cr)
            columnInfos.put(rawColumnName, ci)
            ci
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
            case ColumnInfo(cr) =>
              TypeChecker.check(cr.typ, uVal) match {
                case Right(v) => (cr.id.underlying -> uVal) :: Nil
                case Left(TypeChecker.Error(expected, got)) => throw MaltypedDataEx(cr.fieldName, expected, got)
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
          row.get(dataset.primaryKey.underlying) match {
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
      case Some(datasetRecord) =>
        val trans = new RowDataTranslator(datasetRecord, ignoreUnknownColumns = false)
        val upserts = data.map(trans.convert)
        try {
          dc.update(datasetRecord.systemId, datasetRecord.schemaHash, user, instructions ++ upserts) {
            case DataCoordinatorClient.Success(result) =>
              f(StreamSuccess(result))
            case DataCoordinatorClient.SchemaOutOfDate(newSchema) =>
              // hm, if we get schema out of date here, we're pretty much out of luck, since we'll
              // have used up "upserts".  Unless we want to spool it to disk, but for something
              // that SHOULD occur with only low probability that's pretty expensive.
              //
              // I guess we'll refresh our own schema and then toss an error to the user?
              store.resolveSchemaInconsistency(datasetRecord.systemId, newSchema)
              f(SchemaOutOfSync)
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
        f(NotFound(resourceName))
    }
  }

  def upsert[T](resourceName: ResourceName, data: Iterator[JValue])(f: UpsertResult => T): T =
    doUpsertish(resourceName, data, Iterator.empty, f)

  def replace[T](resourceName: ResourceName, data: Iterator[JValue])(f: UpsertResult => T): T =
    doUpsertish(resourceName, data, Iterator.single(RowUpdateOptionChange(truncate = true)), f)
}
