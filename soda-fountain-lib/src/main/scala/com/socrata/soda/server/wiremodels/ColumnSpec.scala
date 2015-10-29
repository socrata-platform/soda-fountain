package com.socrata.soda.server.wiremodels


import scala.{collection => sc}
import com.rojoma.json.v3.ast.{JValue, JObject}
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, Strategy, JsonKeyStrategy}
import com.socrata.soql.environment.{TypeName, ColumnName}
import com.socrata.soda.server.id.ColumnId
import com.socrata.soda.server.util.AdditionalJsonCodecs._
import com.socrata.soql.types.SoQLType
import InputUtils._
import com.socrata.soda.server.errors.{ColumnSpecMaltyped, ColumnSpecUnknownType}

@JsonKeyStrategy(Strategy.Underscore)
case class ColumnSpec(id: ColumnId,
                      fieldName: ColumnName,
                      name:String,
                      description:String,
                      datatype: SoQLType,
                      computationStrategy: Option[ComputationStrategySpec] = None)

object ColumnSpec {
  implicit val jsonCodec = AutomaticJsonCodecBuilder[ColumnSpec]
}

case class ColumnSpecSubSet(id: Option[ColumnId], fieldName: Option[ColumnName])
object ColumnSpecSubSet {
  implicit val jsonCodec = AutomaticJsonCodecBuilder[ColumnSpecSubSet]
}


// A user provided column spec doesn't necessarily have an ID.
// If it does, it's because it's updating an existing column -- that is,
// users cannot select their own column IDs.
case class UserProvidedColumnSpec(id: Option[ColumnId],
                                  fieldName: Option[ColumnName],
                                  name: Option[String],
                                  description:Option[String],
                                  datatype:Option[SoQLType],
                                  delete: Option[Boolean],
                                  computationStrategy: Option[UserProvidedComputationStrategySpec])

object UserProvidedColumnSpec extends UserProvidedSpec[UserProvidedColumnSpec] {
  def fromObject(obj: JObject) : ExtractResult[UserProvidedColumnSpec] = {
    val cex = new ColumnExtractor(obj.fields)
    for {
      columnId <- cex.columnId
      fieldName <- cex.fieldName
      name <- cex.name
      description <- cex.description
      datatype <- cex.datatype
      delete <- cex.delete
      computationStrategy <- cex.computationStrategy
    } yield {
      UserProvidedColumnSpec(columnId, fieldName, name, description, datatype, delete, computationStrategy)
    }
  }

  // Using this class instead of AutomaticJsonCodecBuilder allows us to
  // return a specific SodaError citing what part of the extraction failed.
  class ColumnExtractor(map: sc.Map[String, JValue]) {
    val context = new ExtractContext(ColumnSpecMaltyped)
    import context._

    private def e[T : Decoder](field: String): ExtractResult[Option[T]] =
      extract[T](map, field)

    def columnId = e[ColumnId]("id")
    def name = e[String]("name")
    def fieldName = e[ColumnName]("field_name")
    def datatype = e[TypeName]("datatype").flatMap {
      case Some(typeName) =>
        SoQLType.typesByName.get(typeName) match {
          case Some(typ) => Extracted(Some(typ))
          case None      => RequestProblem(ColumnSpecUnknownType(typeName))
        }
      case None =>
        Extracted(None)
    }
    def description = e[String]("description")
    def delete = e[Boolean]("delete")
    def computationStrategy: ExtractResult[Option[UserProvidedComputationStrategySpec]] =
      e[JObject]("computation_strategy") match {
        case Extracted(Some(jobj)) => UserProvidedComputationStrategySpec.fromObject(jobj).map(Some(_))
        case _ => Extracted(None)
      }
  }

}