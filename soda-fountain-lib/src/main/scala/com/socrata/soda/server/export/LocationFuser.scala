package com.socrata.soda.server.export

import com.rojoma.json.v3.ast.JObject
import com.rojoma.json.v3.codec._
import com.rojoma.json.v3.conversions._
import com.socrata.soda.server.highlevel.ExportDAO
import com.socrata.soda.server.util.AdditionalJsonCodecs._
import com.socrata.soda.server.wiremodels.{JsonColumnRep, JsonColumnWriteRep}
import com.socrata.soql.types._

import com.rojoma.json.util.JsonUtil

class LocationFuser(schema: ExportDAO.CSchema, reps: Array[JsonColumnWriteRep], loc: String) {

  val fieldNameIdxMap = schema.schema.zipWithIndex.map {
    case (columnInfo, idx) => (columnInfo.fieldName.caseFolded, idx)
  }.toMap


  val LocStreet = loc + "_address"
  val LocCity = loc + "_city"
  val LocState = loc + "_state"
  val LocZip = loc + "_zip"

  val pointIdx = fieldNameIdxMap(loc)
  val streetIdx = fieldNameIdxMap(LocStreet)
  val cityIdx = fieldNameIdxMap(LocCity)
  val stateIdx = fieldNameIdxMap(LocState)
  val zipIdx = fieldNameIdxMap(LocZip)

  reps(pointIdx) = JsonColumnRep.LocationRep

  def convertSchema(schema: ExportDAO.CSchema): ExportDAO.CSchema = {
    schema.copy(schema = schema.schema.foldLeft(Seq.empty[ExportDAO.ColumnInfo]) { (acc, ci) =>
      ci.fieldName.caseFolded match {
        case x if x == loc =>
          acc :+ ci.copy(typ = SoQLLocation, computationStrategy = None)
        case LocStreet | LocCity | LocState | LocZip =>
          acc
        case _ =>
          acc :+ ci
      }
    })
  }

  def convert(row: Array[SoQLValue]): Unit = {
    val latLon = row(pointIdx)
    val street = row(streetIdx)
    val city = row(cityIdx)
    val state = row(stateIdx)
    val zip = row(zipIdx)

    val address = JObject(Map(
      "address" -> JsonColumnRep.TextRep.toJValue(street),
      "city" -> JsonColumnRep.TextRep.toJValue(city),
      "state" -> JsonColumnRep.TextRep.toJValue(state),
      "zip" -> JsonColumnRep.TextRep.toJValue(zip)))
    val addressStr = Option(JsonUtil.renderJson(address))

    val svLoc: SoQLValue = latLon match {
      case p@SoQLPoint(pt) =>
        val coord = pt.getCoordinate
        new SoQLLocation(Option(BigDecimal(coord.x).bigDecimal), Option(BigDecimal(coord.y).bigDecimal), addressStr)
      case SoQLNull =>
        new SoQLLocation(None, None, addressStr)
      case _ =>
        SoQLNull
    }
    row(pointIdx) = svLoc
  }
}
