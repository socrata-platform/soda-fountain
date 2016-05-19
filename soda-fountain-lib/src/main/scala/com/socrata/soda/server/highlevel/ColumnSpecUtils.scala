package com.socrata.soda.server.highlevel

import com.rojoma.json.v3.util.WrapperJsonDecode
import com.socrata.computation_strategies._
import com.socrata.soda.server.wiremodels._
import com.socrata.soql.brita.IdentifierFilter
import com.socrata.soql.environment.ColumnName
import com.socrata.soda.server.id.ColumnId
import com.socrata.soql.types.{SoQLType, SoQLFixedTimestamp, SoQLVersion, SoQLID}
import com.socrata.soql.types.obfuscation.Quadifier
import scala.util.Random

import ColumnSpecUtils._

class ColumnSpecUtils(rng: Random) {
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[ColumnSpecUtils])

  def validColumnName(columnName: ColumnName, uCompStrategy: Option[UserProvidedComputationStrategySpec]): Boolean = {
    // Computed columns must start with : to prevent being returned in a SELECT * call.
    // In the future, we could enhance computed columns to have a more distinct identity from
    // system columns, but for now we decided to enforce this rule so that computed columns
    // behave like system columns (not writable thru API, not returned in SELECT *).
    val isComputedSystemLike = uCompStrategy match {
      case Some(UserProvidedComputationStrategySpec(Some(typ), _, _)) => !StrategyType.userColumnAllowed(typ)
      case _ => false // worry about no ComputationStrategyType later...
    }
    if (isComputedSystemLike &&
         (!columnName.name.startsWith(":") ||
         systemColumns.exists { sysCol => columnName.name.equalsIgnoreCase(sysCol._1.name) }))
      return false

    val cnamePart =
      if (columnName.name.startsWith(":@")) ColumnName(columnName.name.substring(2))
      else if (isComputedSystemLike) ColumnName(columnName.name.substring(1))
      else columnName
    IdentifierFilter(cnamePart.name) == cnamePart.name
  }
  
  def duplicateColumnName(columnName: ColumnName, existingColumns: Map[ColumnName, ColumnId]): Boolean =
    existingColumns.map(_._1).exists(_.name.equalsIgnoreCase(columnName.name))

  def freezeForCreation(existingColumns: Map[ColumnName, (ColumnId, SoQLType)], ucs: UserProvidedColumnSpec): CreateResult =
    ucs match {
      case UserProvidedColumnSpec(None, Some(fieldName), Some(typ), None, uCompStrategy) =>
        if (!validColumnName(fieldName, uCompStrategy)) return InvalidFieldName(fieldName)
        val existingColumnIds = existingColumns.mapValues(_._1)
        if (duplicateColumnName(fieldName, existingColumnIds)) return DuplicateColumnName(fieldName)
        val id = selectId(existingColumnIds.values)
        freezeForCreation(existingColumns, typ, uCompStrategy) match {
          case ComputationStrategySuccess(compStrategy) =>
            Success(ColumnSpec(id, fieldName, typ, compStrategy))
          case cr: CreateResult => cr
        }
      case UserProvidedColumnSpec(Some(_), _, _, _, _) =>
        IdGiven
      case UserProvidedColumnSpec(_, None, _, _, _) =>
        NoFieldName
      case UserProvidedColumnSpec(_, _, None, _, _) =>
        NoType
      case UserProvidedColumnSpec(_, _, _, Some(_), _) =>
        DeleteSet
    }

  def freezeForCreation(existingColumns: Map[ColumnName, (ColumnId, SoQLType)],
                        datatype: SoQLType,
                        ucs: Option[UserProvidedComputationStrategySpec]): CreateResult =
    ucs match {
      case Some(UserProvidedComputationStrategySpec(Some(typ), sourceColumns, parameters)) =>
        val definition = StrategyDefinition[ColumnName](typ, sourceColumns.map(_.map(new ColumnName(_))), parameters)

        // Column should have the correct datatype
        val targetDatatype = ComputationStrategy.strategies(definition.typ).targetColumnType
        if (datatype != targetDatatype)
          return WrongDatatypeForComputationStrategy(datatype, targetDatatype)

        // Validate the strategy and the datatypes of the source columns
        implicit val decode = WrapperJsonDecode[ColumnName]{ name: String => new ColumnName(name): ColumnName }
        ComputationStrategy.validate[ColumnName](definition, existingColumns.mapValues(_._2)) match {
          case Some(UnknownSourceColumn(_)) =>
            UnknownComputationStrategySourceColumn
          case Some(other) =>
            InvalidComputationStrategy(other)
          case None =>
            // The logic below assumes that the computed column is defined after the source column in the schema.
            // TODO : Validation should be independent of column ordering in the schema definition.
            val sourceColumnSpecs = sourceColumns.map(_.map(sourceColumnSpec(_, existingColumns.mapValues(_._1))))
            sourceColumnSpecs match {
              case Some(specs: Seq[Option[SourceColumnSpec]]) =>
                ComputationStrategySuccess(Some(ComputationStrategySpec(typ, Some(specs.flatten), parameters)))
              case None =>
                ComputationStrategySuccess(Some(ComputationStrategySpec(typ, None, parameters)))
            }
        }
      case Some(UserProvidedComputationStrategySpec(None, _, _)) => ComputationStrategyNoStrategyType
      case None => ComputationStrategySuccess(None)
    }

  def sourceColumnSpec(sourceName: String, existingColumns: Map[ColumnName, ColumnId]): Option[SourceColumnSpec] =
    for {
      name <- existingColumns.keySet.find(_.name == sourceName)
      id   <- existingColumns.get(name)
    } yield SourceColumnSpec(id, name)

  def selectId(existingIds: Iterable[ColumnId]): ColumnId = {
    var id = randomId()
    while(existingIds.exists(id == _)) {
      log.info("Wow!  Collided {}", id)
      id = randomId()
    }
    id
  }

  def randomId(): ColumnId = {
    val a = rng.nextLong() & ((1L << 40) - 1)
    new ColumnId(Quadifier.quadify(a.toInt) + "-" + Quadifier.quadify((a >> 20).toInt))
  }

  def systemColumns = {
    // TODO: Grovel system columns out of the data coordinator instead of hardcoding
    _systemColumns
  }

  private[this] val _systemColumns = Map(
    ColumnName(":id") -> ColumnSpec(ColumnId(":id"), ColumnName(":id"), SoQLID, None),
    ColumnName(":version") -> ColumnSpec(ColumnId(":version"), ColumnName(":version"), SoQLVersion, None),
    ColumnName(":created_at") -> ColumnSpec(ColumnId(":created_at"), ColumnName(":created_at"), SoQLFixedTimestamp, None),
    ColumnName(":updated_at") -> ColumnSpec(ColumnId(":updated_at"), ColumnName(":updated_at"), SoQLFixedTimestamp, None)
  )
}

object ColumnSpecUtils {
  sealed abstract class CreateResult
  case class Success(spec: ColumnSpec) extends CreateResult
  case class ComputationStrategySuccess(spec: Option[ComputationStrategySpec]) extends CreateResult
  case object IdGiven extends CreateResult
  case object NoFieldName extends CreateResult
  case class InvalidFieldName(name: ColumnName) extends CreateResult
  case class DuplicateColumnName(name: ColumnName) extends CreateResult
  case object NoType extends CreateResult
  case object DeleteSet extends CreateResult
  case object UnknownComputationStrategySourceColumn extends CreateResult
  case object ComputationStrategyNoStrategyType extends CreateResult
  case class WrongDatatypeForComputationStrategy(found: SoQLType, required: SoQLType) extends CreateResult
  case class InvalidComputationStrategy(error: ValidationError) extends CreateResult

  def isSystemColumn(columnName: ColumnName): Boolean = columnName.name.startsWith(":")
}
