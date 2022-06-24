package com.socrata.soda.server.highlevel

import com.socrata.soda.server.copy.Published
import com.socrata.soda.server.id.{ColumnId, ResourceName}
import com.socrata.soda.server.persistence.{DatasetRecord, MinimalDatasetRecord, NameAndSchemaStore}
import com.socrata.soql.ast._
import com.socrata.soql.collection.OrderedMap
import com.socrata.soql.environment.{ColumnName, TableName}
import com.socrata.soql.functions.{SoQLFunctionInfo, SoQLTypeInfo}
import com.socrata.soql.mapping.ColumnNameMapper
import com.socrata.soql.parsing.{AbstractParser, Parser}
import com.socrata.soql.types.SoQLType
import com.socrata.soql.{BinaryTree, Compound, Leaf, SoQLAnalyzer}


object RollupHelper {

  private val soqlAnalyzer: SoQLAnalyzer[SoQLType] = new SoQLAnalyzer(SoQLTypeInfo, SoQLFunctionInfo)

  def parse(soql: String): (BinaryTree[Select], Set[TableName]) = {
    val parsedQueries = new Parser(AbstractParser.defaultParameters).binaryTreeSelect(soql)
    val tableNames = collectTableNames(parsedQueries).map(name => TableName(name))
    (parsedQueries, tableNames)
  }

  /**
   * Validate rollup soql via soql analyze
   */
  def validate(store: NameAndSchemaStore, resourceName: ResourceName, parsedQueries: BinaryTree[Select], tableNames: Set[TableName]): Unit = {

    val initContext =
      store.lookupDataset(resourceName, Some(Published)).orElse(store.lookupDataset(resourceName, store.latestCopyNumber(resourceName))).toSeq.map { datasetRecord =>
        TableName.PrimaryTable.qualifier -> toDatasetContext(datasetRecord)
      }.toMap

    val contexts = tableNames.toSeq.foldLeft(initContext) { (acc, tableName) =>
      val resourceName = new ResourceName(tableName.nameWithSodaFountainPrefix)
      store.lookupDataset(resourceName, Some(Published)).orElse(store.lookupDataset(resourceName, store.latestCopyNumber(resourceName))) match {
        case Some(datasetRecord) =>
          acc + (tableName.nameWithSodaFountainPrefix -> toDatasetContext(datasetRecord))
        case None => acc
      }
    }

    soqlAnalyzer.analyzeBinary(parsedQueries)(contexts)
  }

  /**
   * mapping soql from user column id to internal column id
   */
  def mapQuery(store: NameAndSchemaStore, resourceName: ResourceName, parsedQueries: BinaryTree[Select], tableNames: Set[TableName],
               columnNameMap: MinimalDatasetRecord => Map[ColumnName, ColumnName] = columnNameMap,
               generateAliases: Boolean = false): String = {

    val context: Map[String, Map[ColumnName, ColumnName]] = store.translateResourceName(resourceName).map { ds =>
      TableName.PrimaryTable.qualifier -> columnNameMap(ds)
    }.toMap

    val mapperContexts = tableNames.foldLeft(context) { (acc, tableName) =>
      val tn = tableName.nameWithSodaFountainPrefix
      val resource = new ResourceName(tn)
      store.translateResourceName(resource) match {
        case Some(ds) =>
          acc + (tn -> columnNameMap(ds))
        case None =>
          acc
      }
    }

    val mapper = new ColumnNameMapper(mapperContexts)
    val mappedAst = mapper.mapSelects(parsedQueries, generateAliases)
    mappedAst.toString
  }

  /**
   * mapping soql from internal column id to user column id
   */
  def reverseMapQuery(store: NameAndSchemaStore, resourceName: ResourceName, parsedQueries: BinaryTree[Select], tableNames: Set[TableName]): String = {
    mapQuery(store, resourceName, parsedQueries, tableNames, reverseColumnNameMap, false)
  }

  private def columnNameMap(dsRecord: MinimalDatasetRecord): Map[ColumnName, ColumnName] = {
    dsRecord.columnsByName.mapValues(col => rollupColumnIdToColumnNameMapping(col.id))
  }

  private def reverseColumnNameMap(dsRecord: MinimalDatasetRecord): Map[ColumnName, ColumnName] = {
    dsRecord.columns.map { columnRecord =>
      (rollupColumnIdToColumnNameMapping(columnRecord.id), columnRecord.fieldName)
    }.toMap
  }

  private def rollupColumnIdToColumnNameMapping(cid: ColumnId): ColumnName = {
    val name = cid.underlying
    name(0) match {
      case ':' => new ColumnName(name)
      case _ => new ColumnName("_" + name)
    }
  }

  private def toDatasetContext(datasetRecord: DatasetRecord): com.socrata.soql.environment.DatasetContext[SoQLType] = {
    val columnTypes = datasetRecord.columns.map { column =>
      (column.fieldName, column.typ)
    }
    val context = new com.socrata.soql.environment.DatasetContext[SoQLType] {
      val schema: OrderedMap[ColumnName, SoQLType] = OrderedMap(columnTypes: _*)
    }
    context
  }

  def collectTableNames(selects: BinaryTree[Select]): Set[String] = {
    selects match {
      case Compound(_, l, r) =>
        collectTableNames(l) ++ collectTableNames(r)
      case Leaf(select) =>
        select.joins.foldLeft(select.from.map(_.name).filter(_ != TableName.This).toSet) { (acc, join) =>
          join.from match {
            case JoinTable(TableName(name, _)) =>
              acc + name
            case JoinQuery(selects, _) =>
              acc ++ collectTableNames(selects)
            case JoinFunc(_, _) =>
              throw new Exception("Unexpected join function")
          }
        }
    }
  }
}