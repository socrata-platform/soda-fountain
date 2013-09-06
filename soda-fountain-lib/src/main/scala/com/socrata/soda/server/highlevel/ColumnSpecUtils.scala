package com.socrata.soda.server.highlevel

import scala.util.Random
import com.socrata.soda.server.wiremodels.{ColumnSpec, UserProvidedColumnSpec}
import com.socrata.soql.environment.ColumnName
import com.socrata.soda.server.id.ColumnId
import com.socrata.soql.types.obfuscation.Quadifier

import ColumnSpecUtils._
import com.socrata.soql.brita.IdentifierFilter

class ColumnSpecUtils(rng: Random) {
  private val log = org.slf4j.LoggerFactory.getLogger(classOf[ColumnSpecUtils])

  def validColumnName(columnName: ColumnName) = IdentifierFilter(columnName.name) == columnName.name

  def freezeForCreation(existingColumns: Map[ColumnName, ColumnId], ucs: UserProvidedColumnSpec): CreateResult =
    ucs match {
      case UserProvidedColumnSpec(None, Some(fieldName), Some(name), desc, Some(typ), None) =>
        if(!validColumnName(fieldName)) return InvalidFieldName(fieldName)
        val trueDesc = desc.getOrElse("")
        val id = selectId(existingColumns.values)
        Success(ColumnSpec(id, fieldName, name, trueDesc, typ))
      case UserProvidedColumnSpec(Some(_), _, _, _, _, _) =>
        IdGiven
      case UserProvidedColumnSpec(_, None, _, _, _, _) =>
        NoFieldName
      case UserProvidedColumnSpec(_, _, None, _, _, _) =>
        NoName
      case UserProvidedColumnSpec(_, _, _, _, None, _) =>
        NoType
      case UserProvidedColumnSpec(_, _, _, _, _, Some(_)) =>
        DeleteSet
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
}

object ColumnSpecUtils {
  sealed abstract class CreateResult
  case class Success(spec: ColumnSpec) extends CreateResult
  case object IdGiven extends CreateResult
  case object NoFieldName extends CreateResult
  case class InvalidFieldName(name: ColumnName) extends CreateResult
  case object NoName extends CreateResult
  case object NoType extends CreateResult
  case object DeleteSet extends CreateResult
}
