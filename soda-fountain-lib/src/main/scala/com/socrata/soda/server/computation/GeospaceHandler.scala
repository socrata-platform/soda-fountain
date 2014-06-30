package com.socrata.soda.server.computation

import com.rojoma.json.ast.{JObject, JString, JArray, JNumber}
import com.rojoma.json.codec.JsonCodec
import com.rojoma.json.io.{JsonReader, CompactJsonWriter}
import com.socrata.soda.server.highlevel.RowDataTranslator
import com.socrata.soda.server.highlevel.RowDataTranslator.{DeleteAsCJson, UpsertAsSoQL}
import com.socrata.soda.server.persistence._
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.{SoQLPoint, SoQLText}
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

  // Get config values
  // TODO: use ZK/Curator to discover Geospace
  val geospaceHost = Try(config.getString("host")).getOrElse("localhost")
  val geospacePort = Try(config.getInt("port")).getOrElse(2020)
  val batchSize    = Try(config.getInt("batch-size")).getOrElse(200)

  private val urlPrefix = s"http://${geospaceHost}:${geospacePort}/experimental"
  private val logger = LoggerFactory.getLogger(getClass)

  case class Point(x: Double, y: Double)

  /**
   * A single-threaded (for now) geo-region-coding handler.  Batches and sends out the points
   * to Geospace, then incorporates the feature IDs into a new column.
   *
   * == Parsing from a single point column ==
   * This is the only supported source column for now.
   * sourceColumns must be a list of one column, and it must be a Geo Point type.
   * parameters: {"region":  <<name of geo region dataset 4x4>>}
   */
  def compute(sourceIt: Iterator[RowDataTranslator.Success], column: MinimalColumnRecord): Iterator[RowDataTranslator.Success] = {
    // Only a single point column is allowed as a source for now
    val (geoColumnName, region) = parsePointColumnSourceStrategy(column)

    val batches = sourceIt.grouped(batchSize)
    val computedBatches = batches.map { batch =>
      val rows = batch.toSeq

      // Grab just the upserts and get the point column for mapping to feature ID
      val points = rows.collect {
        case upsert: UpsertAsSoQL => extractPointFromRow(upsert.rowData.toMap, ColumnName(geoColumnName))
      }

      // Convert points to feature IDs, and splice IDs back into rows.
      // Deletes are returned untouched.
      val featureIds = if (points.size > 0) geospaceRegionCoder(points, region).iterator else Iterator[String]()
      rows.map {
        case upsert: UpsertAsSoQL  =>
          if (featureIds.hasNext) {
            UpsertAsSoQL(upsert.rowData + (column.fieldName.name -> SoQLText(featureIds.next)))
          } else {
            val message = "Not enough featureIds returned by Geospace"
            logger.error(message)
            throw ComputationEx(message, None)
          }
        case delete: DeleteAsCJson => delete
        case _                     =>
          val message = "Unsupported row update type passed into GeospaceHandler"
          logger.error(message)
          throw ComputationEx(message, None)
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

  private def extractPointFromRow(rowmap: SoQLRow, colName: ColumnName): Point = {
    val pointSoql = rowmap.get(colName.name).getOrElse(throw UnknownColumnEx(colName))
    pointSoql match {
      case point: SoQLPoint => Point(point.value.getX, point.value.getY)
      case x                => throw MaltypedDataEx(colName, SoQLPoint, pointSoql.typ)
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