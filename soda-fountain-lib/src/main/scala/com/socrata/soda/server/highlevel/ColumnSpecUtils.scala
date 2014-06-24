package com.socrata.soda.server.highlevel

import com.socrata.soda.server.wiremodels.{ComputationStrategySpec, UserProvidedComputationStrategySpec, ColumnSpec, UserProvidedColumnSpec}
import com.socrata.soql.brita.IdentifierFilter
import com.socrata.soql.environment.ColumnName
import com.socrata.soda.server.id.ColumnId
import com.socrata.soql.types.{SoQLFixedTimestamp, SoQLVersion, SoQLID}
import com.socrata.soql.types.obfuscation.Quadifier
import scala.util.Random

import ColumnSpecUtils._

class ColumnSpecUtils(rng: Random) {
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[ColumnSpecUtils])

  def validColumnName(columnName: ColumnName, isComputed: Boolean): Boolean = {
    // Computed columns must start with : to prevent being returned in a SELECT * call.
    // In the future, we could enhance computed columns to have a more distinct identity from
    // system columns, but for now we decided to enforce this rule so that computed columns
    // behave like system columns (not writable thru API, not returned in SELECT *).
    if (isComputed &&
        (!columnName.name.startsWith(":") ||
         systemColumns.exists { sysCol => columnName.name.equalsIgnoreCase(sysCol._1.name) }))
      return false

    val cnamePart =
      if (columnName.name.startsWith(":@")) ColumnName(columnName.name.substring(2))
      else if (isComputed) ColumnName(columnName.name.substring(1))
      else columnName
    IdentifierFilter(cnamePart.name) == cnamePart.name
  }

  def freezeForCreation(existingColumns: Map[ColumnName, ColumnId], ucs: UserProvidedColumnSpec): CreateResult =
    ucs match {
      case UserProvidedColumnSpec(None, Some(fieldName), Some(name), desc, Some(typ), None, uCompStrategy) =>
        if(!validColumnName(fieldName, uCompStrategy.isDefined)) return InvalidFieldName(fieldName)
        val trueDesc = desc.getOrElse("")
        val id = selectId(existingColumns.values)
        freezeForCreation(uCompStrategy) match {
          case ComputationStrategySuccess(compStrategy) =>
            Success(ColumnSpec(id, fieldName, name, trueDesc, typ, compStrategy))
          case cr: CreateResult => cr
        }
      case UserProvidedColumnSpec(Some(_), _, _, _, _, _, _) =>
        IdGiven
      case UserProvidedColumnSpec(_, None, _, _, _, _, _) =>
        NoFieldName
      case UserProvidedColumnSpec(_, _, None, _, _, _, _) =>
        NoName
      case UserProvidedColumnSpec(_, _, _, _, None, _, _) =>
        NoType
      case UserProvidedColumnSpec(_, _, _, _, _, Some(_), _) =>
        DeleteSet
    }

  def freezeForCreation(ucs: Option[UserProvidedComputationStrategySpec]): CreateResult =
    ucs match {
      case Some(UserProvidedComputationStrategySpec(Some(typ), Some(recompute), sourceColumns, parameters)) =>
        ComputationStrategySuccess(Some(ComputationStrategySpec(typ, recompute, sourceColumns, parameters)))
      case Some(UserProvidedComputationStrategySpec(None, _, _, _)) => InvalidComputationStrategy
      case Some(UserProvidedComputationStrategySpec(_, None, _, _)) => InvalidComputationStrategy
      case None => ComputationStrategySuccess(None)
    }

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
    log.info("TODO: Grovel system columns out of the data coordinator instead of hardcoding")
    _systemColumns
  }

  private[this] val _systemColumns = Map(
    ColumnName(":id") -> ColumnSpec(ColumnId(":id"), ColumnName(":id"), ":id", "", SoQLID, None),
    ColumnName(":version") -> ColumnSpec(ColumnId(":version"), ColumnName(":version"), ":version", "", SoQLVersion, None),
    ColumnName(":created_at") -> ColumnSpec(ColumnId(":created_at"), ColumnName(":created_at"), ":created_at", "", SoQLFixedTimestamp, None),
    ColumnName(":updated_at") -> ColumnSpec(ColumnId(":updated_at"), ColumnName(":updated_at"), ":updated_at", "", SoQLFixedTimestamp, None)
  )
}

object ColumnSpecUtils {
  sealed abstract class CreateResult
  case class Success(spec: ColumnSpec) extends CreateResult
  case class ComputationStrategySuccess(spec: Option[ComputationStrategySpec]) extends CreateResult
  case object IdGiven extends CreateResult
  case object NoFieldName extends CreateResult
  case class InvalidFieldName(name: ColumnName) extends CreateResult
  case object NoName extends CreateResult
  case object NoType extends CreateResult
  case object DeleteSet extends CreateResult
  case object InvalidComputationStrategy extends CreateResult
}
