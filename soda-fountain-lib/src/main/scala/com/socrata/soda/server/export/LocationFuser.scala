package com.socrata.soda.server.export

import com.rojoma.json.v3.ast.JObject
import com.rojoma.json.v3.codec._
import com.rojoma.json.v3.conversions._
import com.socrata.soda.server.highlevel.ExportDAO
import com.socrata.soda.server.util.AdditionalJsonCodecs._
import com.socrata.soda.server.wiremodels.{JsonColumnRep, JsonColumnWriteRep}
import com.socrata.soql.types._

import com.rojoma.json.util.JsonUtil

/**
  * Ouch, this class mutates the reps
  */
class LocationFuser(schema: ExportDAO.CSchema, reps: Array[JsonColumnWriteRep], loc: String) {

  private val fieldNameIdxMap = schema.schema.zipWithIndex.map {
    case (columnInfo, idx) => (columnInfo.fieldName.caseFolded, idx)
  }.toMap

  private val LocStreet = loc + "_address"
  private val LocCity = loc + "_city"
  private val LocState = loc + "_state"
  private val LocZip = loc + "_zip"

  private val pointIdx = fieldNameIdxMap(loc)
  private val streetIdx = fieldNameIdxMap(LocStreet)
  private val cityIdx = fieldNameIdxMap(LocCity)
  private val stateIdx = fieldNameIdxMap(LocState)
  private val zipIdx = fieldNameIdxMap(LocZip)

  // ouch, mutate the reps
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

  /**
    * This function mutate the row
    */
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
    // ouch, mutate the row
    row(pointIdx) = svLoc
  }
}
