package com.socrata.soda.server.highlevel

import com.socrata.soda.server.id.{ColumnId, ResourceName}
import com.socrata.soda.server.persistence.{MinimalDatasetRecord, NameAndSchemaStore}
import com.socrata.soql.ast._
import com.socrata.soql.environment.{ColumnName, TableName}
import com.socrata.soql.mapping.ColumnNameMapper
import com.socrata.soql.parsing.StandaloneParser
import com.socrata.soql.{BinaryTree, Compound, Leaf}


object ColumnNameMapperHelper {
  /**
   * mapping soql from user column id to internal column id
   */
  def mapQuery(store: NameAndSchemaStore, resourceName: ResourceName, soql: String,
               columnNameMap: MinimalDatasetRecord => Map[ColumnName, ColumnName] = columnNameMap): String = {
    val parsedQueries = new StandaloneParser().binaryTreeSelect(soql)
    val tableNames = collectTableNames(parsedQueries)
    val context: Map[String, Map[ColumnName, ColumnName]] = store.translateResourceName(resourceName).map { ds =>
      TableName.PrimaryTable.qualifier -> columnNameMap(ds)
    }.toMap

    val mapperContexts = tableNames.foldLeft(context) { (acc, tn) =>
      val resource = new ResourceName(tn)
      store.translateResourceName(resource) match {
        case Some(ds) =>
          acc + (tn -> columnNameMap(ds))
        case None =>
          acc
      }
    }

    val mapper = new ColumnNameMapper(mapperContexts)
    val mappedAst = mapper.mapSelects(parsedQueries, true)
    mappedAst.toString
  }

  /**
   * mapping soql from internal column id to user column id
   */
  def reverseMapQuery(store: NameAndSchemaStore, resourceName: ResourceName, soql: String): String = {
    mapQuery(store, resourceName, soql, reverseColumnNameMap)
  }

  private def columnNameMap(dsRecord: MinimalDatasetRecord): Map[ColumnName, ColumnName] = {
    dsRecord.columnsByName.mapValues(col => rollupColumnNameToIdMapping(col.id))
  }

  private def reverseColumnNameMap(dsRecord: MinimalDatasetRecord): Map[ColumnName, ColumnName] = {
    dsRecord.columns.map { columnRecord =>
      (rollupColumnNameToIdMapping(columnRecord.id), columnRecord.fieldName)
    }.toMap
  }

  private def rollupColumnNameToIdMapping(cid: ColumnId): ColumnName = {
    val name = cid.underlying
    name(0) match {
      case ':' => new ColumnName(name)
      case _ => new ColumnName("_" + name)
    }
  }

  private def collectTableNames(selects: BinaryTree[Select]): Set[String] = {
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