package com.socrata.soda.server.highlevel

import com.rojoma.json.ast.{JValue, JObject, JString}
import com.socrata.soda.server.persistence._
import com.socrata.soda.server.wiremodels.JsonColumnRep
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.SoQLPoint
import com.typesafe.config.{Config, ConfigFactory}
import scala.util.Try

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

  val computationType = "geospace"

  // Get config values
  // TODO: use ZK/Curator to discover Geospace
  val geospaceHost = Try(config.getString("host")).getOrElse("localhost")
  val geospacePort = Try(config.getInt("port")).getOrElse(2020)
  val batchSize    = Try(config.getInt("batch-size")).getOrElse(200)
  val fakeCoder    = Try(config.getBoolean("fake-coder")).getOrElse(false)

  val pointRep = JsonColumnRep.forClientType(SoQLPoint)

  case class Point(x: Double, y: Double)

  private val coder = if (fakeCoder) fakeRegionCoder(_) else geospaceRegionCoder(_)

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
    // TODO: check the columnRecord once we have urmilan's changes.
    //    It should be a string type
    val geoColumnName = "geom"   // TODO: incorporate urmilan's changes

    val batches = sourceIt.grouped(batchSize)
    val computedBatches = batches.map { batch =>
      val rows = batch.toSeq
      val points = rows.map {
        case JObject(rowmap) => extractPointFromRow(rowmap, ColumnName(geoColumnName))
        // Upserts must have a JSON Map on each row.  This is probably coding error.
        case _  =>  throw new RuntimeException("Data for upserts must consist of a JSON Object per row")
      }

      // Now convert points to feature IDs, and splice IDs back into rows
      val featureIds = coder(points)
      rows.zip(featureIds).map { case (JObject(rowmap), featureId) =>
        JObject(rowmap + (column.fieldName.name -> JString(featureId)))
      }.toIterator
    }
    computedBatches.flatten
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

  // Strictly for testing.  Inputs just get serialized.
  private def fakeRegionCoder(points: Seq[Point]): Seq[String] =
    points.map { case Point(x, y) => s"$x/$y" }

  private def geospaceRegionCoder(points: Seq[Point]): Seq[String] = ???
}