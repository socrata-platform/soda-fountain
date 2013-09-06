package com.socrata.soda.server.wiremodels

import scala.{collection => sc}
import com.rojoma.json.util.{AutomaticJsonCodecBuilder, Strategy, JsonKeyStrategy}
import com.socrata.soda.server.id.ColumnId
import com.socrata.soql.environment.{TypeName, ColumnName}
import com.socrata.soql.types.SoQLType
import com.rojoma.json.ast.{JValue, JObject}
import javax.servlet.http.HttpServletRequest

import AdditionalJsonCodecs._
import InputUtils._
import com.socrata.soda.server.errors.{ColumnSpecMaltyped, ColumnSpecUnknownType}

@JsonKeyStrategy(Strategy.Underscore)
case class ColumnSpec(id: ColumnId,
                      fieldName: ColumnName,
                      name:String,
                      description:String,
                      datatype: SoQLType)

object ColumnSpec {
  implicit val jsonCodec = AutomaticJsonCodecBuilder[ColumnSpec]
}

// A user provided column spec doesn't necessarily have an ID.
// If it does, it's because it's updating an existing column -- that is,
// users cannot select their own column IDs.
case class UserProvidedColumnSpec(id: Option[ColumnId],
                                  fieldName: Option[ColumnName],
                                  name: Option[String],
                                  description:Option[String],
                                  datatype:Option[SoQLType],
                                  delete: Option[Boolean])

object UserProvidedColumnSpec {
  def fromRequest(request: HttpServletRequest, approxLimit: Long) : ExtractResult[UserProvidedColumnSpec] =
    catchingInputProblems {
      jsonSingleObjectStream(request, approxLimit) match {
        case Right(jobj) => fromObject(jobj)
        case Left(err) => RequestProblem(err)
      }
    }

  def fromObject(obj: JObject) : ExtractResult[UserProvidedColumnSpec] = {
    val cex = new ColumnExtractor(obj.fields)
    for {
      columnId <- cex.columnId
      fieldName <- cex.fieldName
      name <- cex.name
      description <- cex.description
      datatype <- cex.datatype
      delete <- cex.delete
    } yield {
      UserProvidedColumnSpec(columnId, fieldName, name, description, datatype, delete)
    }
  }

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
          case None => RequestProblem(ColumnSpecUnknownType(typeName))
        }
      case None =>
        Extracted(None)
    }
    def description = e[String]("description")
    def delete = e[Boolean]("delete")
  }

}
