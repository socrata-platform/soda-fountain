package com.socrata.soda.server.computation

import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.conversions._
import com.socrata.soda.server.computation.ComputationHandler.MaltypedDataEx
import com.socrata.soda.server.persistence.{ComputationStrategyRecord, ColumnRecordLike}
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.{SoQLNull, SoQLPoint}
import com.socrata.thirdparty.geojson.JtsCodecs.CoordinateCodec
import com.typesafe.config.Config
import com.vividsolutions.jts.geom.Coordinate
import org.apache.curator.x.discovery.ServiceDiscovery

/**
 * A [[ComputationHandler]] that uses Geospace to match a row to a georegion,
 * based on the value of a specified point column. The georegion feature ID
 * returned for each row represents the georegion whose shape contains the point value.
 * @param config    Configuration information for connecting to Geospace
 * @param discovery ServiceDiscovery instance used for discovering other services using ZK/Curator
 * @tparam T        ServiceDiscovery payload type
 */
class GeoregionMatchOnPointHandler[T](config: Config, discovery: ServiceDiscovery[T])
  extends GeoregionMatchHandler[T, Coordinate](config, discovery) {

  /**
   * Constructs the Geospace region coding endpoint. Format is:
   * /regions/:resourceName/geocode
   * where :resourceName is the name of the georegion to match against,
   * defined in the computed column parameters as 'region'
   * @param computedColumn Computed column definition
   * @return               Geospace endpoint for georegion coding against points
   */
  protected def genEndpoint(computedColumn: ColumnRecordLike): String = {
    require(computedColumn.computationStrategy.isDefined, "No computation strategy found")
    computedColumn.computationStrategy match {
      case Some(ComputationStrategyRecord(_, _, _, Some(JObject(map)))) =>
        require(map.contains("region"), "parameters does not contain 'region'")
        val JString(region) = map("region")
        s"/regions/$region/geocode"
      case x =>
        throw new IllegalArgumentException("Computation strategy parameters were invalid." +
          """Expected format: { "region" : "{region_resource_name" }""")
    }
  }

  /**
   * Extracts the value of the point column given the key-value map of fields in the row
   * @param rowmap  Map of fields in the row
   * @param colName Name of the point column
   * @return        Value of the source column as a Point(x,y)
   */
  protected def extractSourceColumnValueFromRow(rowmap: SoQLRow, colName: ColumnName): Option[Coordinate] =
    rowmap.get(colName.name) match {
      case Some(point: SoQLPoint) => Some(new Coordinate(point.value.getX, point.value.getY))
      case Some(SoQLNull)         => None
      case Some(x)                => throw MaltypedDataEx(colName, SoQLPoint, x.typ)
      case None                   => None
    }

  /**
   * Serializes a point to a JSON format that Geospace understands
   * eg. Point(1,1) would be converted to [1,1]
   * @param point Point object
   * @return      Point value in the format expected by Geospace
   */
  protected def toJValue(point: Coordinate): JValue = CoordinateCodec.encode(point).toV3
}
