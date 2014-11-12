package com.socrata.soda.server.computation

import com.rojoma.json.ast._
import com.socrata.soda.server.computation.ComputationHandler.MaltypedDataEx
import com.socrata.soda.server.persistence.{ComputationStrategyRecord, ColumnRecordLike}
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.{SoQLNull, SoQLPoint}
import com.typesafe.config.Config
import org.apache.curator.x.discovery.ServiceDiscovery

/**
 * Represents a [x,y] location to be georegion coded
 * @param x Longitude
 * @param y Latitude
 */
case class Point(x: Double, y: Double)

/**
 * A [[ComputationHandler]] that uses Geospace to match a row to a georegion,
 * based on the value of a specified point column. The georegion feature ID
 * returned for each row represents the georegion whose shape contains the point value.
 * @param config    Configuration information for connecting to Geospace
 * @param discovery ServiceDiscovery instance used for discovering other services using ZK/Curator
 * @tparam T        ServiceDiscovery payload type
 */
class GeoregionMatchOnPointHandler[T](config: Config, discovery: ServiceDiscovery[T])
  extends GeoregionMatchHandler[T, Point](config, discovery) {

  /**
   * Constructs the Geospace region coding endpoint. Format is:
   * /regions/:resourceName/geocode
   * where :resourceName is the name of the georegion to match against,
   * defined in the computed column parameters as 'region'
   * @param column Computed column definition
   * @return       Geospace endpoint for georegion coding against points
   */
  protected def genEndpoint(column: ColumnRecordLike): String = {
    require(column.computationStrategy.isDefined, "Not a target computed column")
    column.computationStrategy match {
      case Some(ComputationStrategyRecord(_, _, _, Some(JObject(map)))) =>
        require(map.contains("region"), "parameters does not contain 'region'")
        val JString(region) = map("region")
        s"/regions/$region/geocode"
      case x =>
        throw new IllegalArgumentException("'region' key was not defined in computation strategy")
    }
  }

  /**
   * Extracts the value of the point column given the key-value map of fields in the row
   * @param rowmap  Map of fields in the row
   * @param colName Name of the point column
   * @return        Value of the source column as a Point(x,y)
   */
  protected def extractSourceColumnValueFromRow(rowmap: SoQLRow, colName: ColumnName): Option[Point] =
    rowmap.get(colName.name) match {
      case Some(point: SoQLPoint) => Some(Point(point.value.getX, point.value.getY))
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
  protected def toJValue(point: Point): JValue = JArray(Seq(JNumber(point.x), JNumber(point.y)))
}
