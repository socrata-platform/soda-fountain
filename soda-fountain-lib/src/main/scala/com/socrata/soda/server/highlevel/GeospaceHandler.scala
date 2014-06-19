package com.socrata.soda.server.highlevel

import com.rojoma.json.ast.{JValue, JObject, JString, JArray, JNumber}
import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.io.{JsonReader, CompactJsonWriter}
import com.socrata.soda.server.persistence._
import com.socrata.soda.server.wiremodels.JsonColumnRep
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.SoQLPoint
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory
import scala.util.Try
import scalaj.http.Http

/**
 * A [[ComputationHandler]] for mapping points (or lat/long pairs) to geo features (point-in-polygon)
 * using the Geospace microservice (http://github.com/socrata/geospace).
 *
 * To instantiate, pass the sub-config only, like so:
 *
 *     val handler = new GeospaceHandler(rootConfig.at("computation.handlers.geospace"))
 *
 * == Config ==
 * {{{
 *   host = "localhost"
 *   port = 2020
 *   batch-size = 200    # Number of rows to send to Geospace server at once
 * }}}
 */
class GeospaceHandler(config: Config = ConfigFactory.empty) extends ComputationHandler {
  import ComputationHandler._

  val computationType = "georegion"

  // Get config values
  // TODO: use ZK/Curator to discover Geospace
  val geospaceHost = Try(config.getString("host")).getOrElse("localhost")
  val geospacePort = Try(config.getInt("port")).getOrElse(2020)
  val batchSize    = Try(config.getInt("batch-size")).getOrElse(200)

  private val urlPrefix = s"http://${geospaceHost}:${geospacePort}/experimental"
  private val logger = LoggerFactory.getLogger(getClass)

  case class Point(x: Double, y: Double)

  private val pointRep = JsonColumnRep.forClientType(SoQLPoint)

  /**
   * A single-threaded (for now) geo-region-coding handler.  Batches and sends out the points
   * to Geospace, then incorporates the feature IDs into a new column.
   *
   * == Parsing from a single point column ==
   * This is the only supported source column for now.
   * sourceColumns must be a list of one column, and it must be a Geo Point type.
   * parameters: {"region":  <<name of geo region dataset 4x4>>}
   */
  def compute(sourceIt: Iterator[JValue], column: MinimalColumnRecord): Iterator[JValue] = {
    // Only a single point column is allowed as a source for now
    val (geoColumnName, region) = parsePointColumnSourceStrategy(column)

    val batches = sourceIt.grouped(batchSize)
    val computedBatches = batches.map { batch =>
      val rows = batch.toSeq
      val points = rows.map {
        case JObject(rowmap) => extractPointFromRow(rowmap, ColumnName(geoColumnName))
        // Upserts must have a JSON Map on each row.  This is probably coding error.
        case _  =>  throw new RuntimeException("Data for upserts must consist of a JSON Object per row")
      }

      // Now convert points to feature IDs, and splice IDs back into rows
      val featureIds = geospaceRegionCoder(points, region)
      rows.zip(featureIds).map { case (JObject(rowmap), featureId) =>
        JObject(rowmap + (column.fieldName.name -> JString(featureId)))
      }.toIterator
    }
    computedBatches.flatten
  }

  private def parsePointColumnSourceStrategy(column: MinimalColumnRecord): (String, String) = {
    require(column.computationStrategy.isDefined, "Not a target computed column")
    column.computationStrategy match {
      case Some(ComputationStrategyRecord(_, _, Some(Seq(sourceCol)), Some(JObject(map)))) =>
        require(map contains "region", "parameters does not contain 'region'")
        val JString(regionName) = map("region")
        (sourceCol, regionName)
      case x =>  throw new IllegalArgumentException("There must be exactly 1 sourceColumn, and " +
                                                    "parameters must have a key 'region'")
    }
  }

  // Note: this conversion from JValue to SoQLType is already done in RowDAOImpl.  Yes we are temporarily
  // repeating it here.  What should really be done is to refactor RowDAO so that we can convert to SoQLTYpe
  // once, then do computation and processing directly on SoQLType, then have RowDAO send it onwards.
  private def extractPointFromRow(rowmap: collection.Map[String, JValue], colName: ColumnName): Point = {
    val pointJValue = rowmap.get(colName.name).getOrElse(throw UnknownColumnEx(colName))
    pointRep.fromJValue(pointJValue) match {
      // TODO: remove the null check once the GeometryRep decoder handles nulls.
      // @urmilan has been told about the bug.
      // (see MaltypedDataEx test case)
      case Some(point: SoQLPoint) if (point.value != null) => Point(point.value.getX, point.value.getY)
      case x                      => throw MaltypedDataEx(colName, pointRep.representedType, pointJValue)
    }
  }

  private def geospaceRegionCoder(points: Seq[Point], region: String): Seq[String] = {
    val url = urlPrefix + s"/regions/$region/geocode"
    logger.debug("HTTP POST [{}] with {} points...", url, points.length)

    val jsonPoints = points.map { case Point(x, y) => JArray(Seq(JNumber(x), JNumber(y))) }
    val (status, _, response) = try {
        Http.postData(url, CompactJsonWriter.toString(JArray(jsonPoints))).
             header("content-type", "application/json").
             asHeadersAndParse(Http.readString)
      } catch {
        case e: scalaj.http.HttpException =>
          logger.error("HTTP Error: ", e)
          throw ComputationEx("HTTP Error reading " + url, Some(e))
      }

    logger.debug("Got back status {}, response [{}]", status, response)
    status match {
      case 200 =>
        JsonCodec[Seq[String]].decode(JsonReader.fromString(response)).
          getOrElse(throw ComputationEx("Error parsing JSON response: " + response, None))
      case sc  =>
        val errorMessage = s"Error: HTTP [$url] got response code $sc, body $response"
        logger.error(errorMessage)
        throw ComputationEx(errorMessage, None)
    }
  }
}