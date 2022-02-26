package com.socrata.soda.server.highlevel

import com.socrata.soda.server.id.{ColumnId, ResourceName}
import com.socrata.soda.server.persistence.{MinimalDatasetRecord, NameAndSchemaStore}
import com.socrata.soql.ast._
import com.socrata.soql.environment.{ColumnName, TableName}
import com.socrata.soql.parsing.StandaloneParser
import com.socrata.soql.{BinaryTree, Compound, Leaf, PipeQuery}

import java.util.NoSuchElementException
import scala.util.parsing.input.{NoPosition, Position}

/**
 * Maps column names in the given AST.  Position information is not updated
 * or retained.
 * This is used for rewriting rollups in soda fountain and does not work for chained queries (only operate on the first element).
 *
 * @param columnNameMap Map from current names to new names.  The map must be defined
 *                      for all column names passed in.
 */
class ColumnNameMapper(rootSchemas: Map[String, Map[ColumnName, ColumnName]]) {

  def mapSelects(selects: BinaryTree[Select], generateAliases: Boolean = false): BinaryTree[Select] = {
    selects match {
      case PipeQuery(l, r) =>
        // previously when pipe query is in seq form,
        // mapSelect only operates on the first element
        val nl = mapSelects(l, true)
        PipeQuery(nl, r)
      case Compound(op, l, r) =>
        val nl = mapSelects(l, generateAliases)
        val nr = mapSelects(r, false)
        Compound(op, nl, nr)
      case Leaf(select) =>
        Leaf(mapSelect(select, generateAliases))
    }
  }

  private def mapSelect(select: Select, generateAliases: Boolean): Select = {
    val ss0 = select.from match {
      case None =>
        rootSchemas.get(TableName.PrimaryTable.qualifier) match {
          case Some(x) =>
            Map(TableName.PrimaryTable.qualifier -> x)
          case None =>
            Map.empty[String, Map[ColumnName, ColumnName]]
        }
      case Some(TableName(name, Some(alias))) if name == TableName.This =>
        rootSchemas.get(TableName.PrimaryTable.qualifier) match {
          case Some(x) =>
            Map(alias -> x)
          case None =>
            Map.empty[String, Map[ColumnName, ColumnName]]
        }
      case Some(tn@TableName(_, alias)) =>
        rootSchemas.get(tn.nameWithSodaFountainPrefix) match {
          case Some(x) =>
            Map(alias.getOrElse(TableName.PrimaryTable.qualifier) -> x,
                alias.getOrElse(tn.nameWithSodaFountainPrefix) -> x)
          case None =>
            Map.empty[String, Map[ColumnName, ColumnName]]
        }
      case _ =>
        Map.empty[String, Map[ColumnName, ColumnName]]
    }

    val ss = select.joins.foldLeft(ss0) { (acc, join) =>
      join.from match {
        case JoinTable(tn@TableName(_, _)) =>
          rootSchemas.get(tn.nameWithSodaFountainPrefix) match {
            case Some(schema) =>
              val key = tn.alias.getOrElse(tn.nameWithSodaFountainPrefix)
              acc + (key -> schema)
            case None =>
              acc
          }
        case _ =>
          acc
      }
    }

    val selection = mapSelection(select.selection, ss, generateAliases)
    select.copy(
      selection = selection,
      joins = select.joins.map(j => mapJoin(j, ss)),
      where = select.where map(mapExpression(_, ss)),
      groupBys = select.groupBys.map(mapExpression(_, ss)),
      having = select.having.map(mapExpression(_, ss)),
      orderBys = select.orderBys.map(mapOrderBy(_, ss)),
    )
  }

  def mapColumnNameAndPosition(s: (ColumnName, Position), columnNameMap:Map[ColumnName, ColumnName]): (ColumnName, Position) = {
    (columnNameMap(s._1), NoPosition)
  }

  def mapStarSelection(s: StarSelection, schemas: Map[String, Map[ColumnName, ColumnName]]): StarSelection = {
    val qualifier = s.qualifier.getOrElse(TableName.PrimaryTable.qualifier)
    val mse = s.exceptions.map(x => mapColumnNameAndPosition(x, schemas(qualifier)))
    StarSelection(s.qualifier, mse)
  }

  def mapSelection(s: Selection, schemas: Map[String, Map[ColumnName, ColumnName]], generateAliases: Boolean = false) = {
    val mes = s.expressions.map(e => mapSelectedExpression(e, schemas, generateAliases))
    val mase =  s.allSystemExcept.map(x => mapStarSelection(x, schemas))
    val maue =  s.allUserExcept.map(x => mapStarSelection(x, schemas))
    Selection(mase, maue, mes)
  }

  private def mapSelectedExpression(se: SelectedExpression, schemas: Map[String, Map[ColumnName, ColumnName]], generateAliases: Boolean): SelectedExpression = {
    // name isn't a column name, but a column alias so no mapping
    val mse = se.name match {
      case Some((aliasName, pos)) => Some (aliasName, NoPosition)
      case None =>
        se.expression match {
          case columnOrAliasRef: ColumnOrAliasRef if generateAliases =>
            Some(columnOrAliasRef.column, NoPosition)
          case _ =>
            None
        }
    }

    SelectedExpression(expression = mapExpression(se.expression, schemas), mse)
  }

  private def mapExpression(e: Expression, schemas: Map[String, Map[ColumnName, ColumnName]]): Expression = {
    e match {
      case NumberLiteral(v) => NumberLiteral(v)(NoPosition)
      case StringLiteral(v) => StringLiteral(v)(NoPosition)
      case BooleanLiteral(v) => BooleanLiteral(v)(NoPosition)
      case NullLiteral() => NullLiteral()(NoPosition)
      case Hole(name) => Hole(name)(NoPosition)
      case e: ColumnOrAliasRef =>
        val qualifier = e.qualifier.getOrElse("_")
        schemas.get(qualifier) match {
          case None =>
            e
          case Some(schema) =>
            schema.get(e.column) match {
              case Some(toColumn) =>
                e.copy(column = toColumn)(e.position)
              case None =>
                throw new NoSuchElementException(s"column not found ${qualifier}.${e.column.name}")
            }
        }
      case e: FunctionCall =>
        val mp = e.parameters.map(p => mapExpression(p, schemas))
        val mw = e.window.map(w => mapWindow(w, schemas))
        val mf = e.filter.map(fi => mapExpression(fi, schemas))
        FunctionCall(e.functionName, mp, mf, mw)(NoPosition, NoPosition)
    }
  }

  private def mapJoin(join: Join, schemas: Map[String, Map[ColumnName, ColumnName]]): Join =  {
    val mappedFrom = join.from match {
      case jq: JoinQuery =>
        jq.copy(selects = mapSelects(jq.selects, true))
      case l =>
        l
    }

    val mappedOn = mapExpression(join.on, schemas)
    join match {
      case j: InnerJoin =>
        j.copy(on = mappedOn)
        InnerJoin(from = mappedFrom, mappedOn, j.lateral)
      case j: LeftOuterJoin =>
        j.copy(from = mappedFrom, on = mappedOn)
      case j: RightOuterJoin =>
        j.copy(from = mappedFrom, on = mappedOn)
      case j: FullOuterJoin =>
        j.copy(from = mappedFrom, on = mappedOn)
    }
  }

  def mapOrderBy(o: OrderBy, schemas: Map[String, Map[ColumnName, ColumnName]]): OrderBy = OrderBy(
    expression = mapExpression(o.expression, schemas),
    ascending = o.ascending,
    nullLast = o.nullLast)

  private def mapWindow(w: WindowFunctionInfo, schemas: Map[String, Map[ColumnName, ColumnName]]): WindowFunctionInfo = {
    val WindowFunctionInfo(partitions, orderings, frames) = w
    WindowFunctionInfo(
      partitions.map(mapExpression(_, schemas)),
      orderings.map(mapOrderBy(_, schemas)),
      frames)
  }
}

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
    val mappedAst = mapper.mapSelects(parsedQueries, false)
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