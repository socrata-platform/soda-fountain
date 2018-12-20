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
  * TODO: Not sure if this will work right for multiple locations.
  */
class LocationFuser(schema: ExportDAO.CSchema, loc: String) {

  private val fieldNameIdxMap = schema.schema.zipWithIndex.map {
    case (columnInfo, idx) => (columnInfo.fieldName.caseFolded, idx)
  }.toMap

  private val LocStreet = loc + "_address"
  private val LocCity = loc + "_city"
  private val LocState = loc + "_state"
  private val LocZip = loc + "_zip"

  private val PointIdx = fieldNameIdxMap(loc)
  private val StreetIdx = fieldNameIdxMap(LocStreet)
  private val CityIdx = fieldNameIdxMap(LocCity)
  private val StateIdx = fieldNameIdxMap(LocState)
  private val ZipIdx = fieldNameIdxMap(LocZip)

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

  def fusedReps(reps: Array[JsonColumnWriteRep]): Array[JsonColumnWriteRep] = {
    var i = 0
    val fused = reps.foldLeft(Array.empty[JsonColumnWriteRep]) { (acc, rep) =>
      val appended = i match {
        case StreetIdx | CityIdx | StateIdx | ZipIdx =>
          acc
        case PointIdx =>
          acc :+ JsonColumnRep.LegacyLocationWriteRep
        case _ =>
          acc :+ rep
      }
      i += 1
      appended
    }
    fused
  }

  def convert(row: Array[SoQLValue]): Array[SoQLValue] = {
    val latLon = row(PointIdx)
    val street = row(StreetIdx)
    val city = row(CityIdx)
    val state = row(StateIdx)
    val zip = row(ZipIdx)

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

    var i = 0
    row.foldLeft(Array.empty[SoQLValue]) { (acc, cell) =>
      val appended = i match {
        case StreetIdx | CityIdx | StateIdx | ZipIdx =>
          acc
        case PointIdx =>
          acc :+ svLoc
        case _ =>
          acc :+ cell
      }
      i += 1
      appended
    }
  }
}
