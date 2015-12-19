package com.socrata.soda.server.computation

import com.rojoma.json.v3.ast.{JString, JObject, JValue, JNull}
import com.socrata.soda.server.id.ColumnId
import com.socrata.soda.server.metrics.MetricCounter
import com.socrata.geocoders.{Geocoder, Address, LatLon}
import com.socrata.soda.server.metrics.Metrics.{GeocodingHandlerMetric, Metric}
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import scala.{collection => sc}
import com.socrata.http.server.util.RequestId._
import com.socrata.soda.server.computation.ComputationHandler.{ComputationEx, MaltypedDataEx}
import com.socrata.soda.server.highlevel.RowDataTranslator
import com.socrata.soda.server.highlevel.RowDataTranslator.{DeleteAsCJson, UpsertAsSoQL}
import com.socrata.soda.server.persistence.{ComputationStrategyRecord, ColumnRecordLike}
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.{SoQLNumber, SoQLText, SoQLNull, SoQLPoint}
import com.vividsolutions.jts.geom.{Point, GeometryFactory, Coordinate}

/**
 * A [[ComputationHandler]] that does geocoding of rows.
 */
class GeocodingHandler(config: Config, geocoder: Geocoder, metricProvider: (Metric => Unit)) extends ComputationHandler {

  private val logger = LoggerFactory.getLogger(getClass)

  private val totalRowsCodedCounter = new MetricCounter()
  private val noMatchRowsCounter = new MetricCounter()
  private val timeCounter = new MetricCounter()

  /**
   * Handles the actual computation.  Must be lazy, otherwise will introduce significant latency
   * and OOM errors for large upserts.  IE, try not to convert the Iterator to a regular Seq or Array.
   *
   * @param sourceIt an Iterator of row updates, includes both upsert and deletes.
   *                 For upserts, each row is a Map[String, SoQLValue], where the key is the
   *                 fieldName and the value is a SoQLValue representation of source data.
   *                 Deletes contain only row PK and should be passed through untouched by the computation handler.
   * @param column a ColumnRecord describing the computation and parameters
   * @return an Iterator[SoQLRow] for the output rows.  One of the keys must containing the output column.
   */
  def compute(requestId: RequestId,
              sourceIt: Iterator[RowDataTranslator.Computable],
              column: ColumnRecordLike): Iterator[RowDataTranslator.Computable] = {
    require(column.computationStrategy.isDefined, "No computation strategy found")
    val columnIds = getColumnIds(column)

    val batches = sourceIt.grouped(geocoder.batchSize)
    val computedBatches = batches.map { batch =>
      val rowsWithIndex = batch.zipWithIndex

      // Grab just the upserts and get the source column values for mapping to point
      val sourceValuesWithIndex = rowsWithIndex.collect {
        case (upsert: UpsertAsSoQL, i) => (extractAddress(upsert.rowData, columnIds), i)
      }

      // Convert addresses to points, and splice points back into rows.
      // Deletes are returned untouched.
      val start = System.currentTimeMillis
      val points = geocode(requestId, sourceValuesWithIndex.map(_._1))
      timeCounter.add(System.currentTimeMillis - start)
      noMatchRowsCounter.add(points.count(_.isEmpty))
      val pointsWithIndex = sourceValuesWithIndex.map(_._2).zip(points).toMap
      rowsWithIndex.map {
        case (upsert: UpsertAsSoQL, i) =>
          totalRowsCodedCounter.increment()
          pointsWithIndex.get(i).flatMap { maybePoint =>
            maybePoint.map { point =>
              UpsertAsSoQL(upsert.rowData + (column.id.underlying -> SoQLPoint(point)))
            }
          }.getOrElse(upsert)
        case (delete: DeleteAsCJson, i) => delete
        case _ =>
          val message = s"Unsupported row update type passed into ${getClass.getSimpleName}"
          logger.error(message)
          throw ComputationEx(message, None)
      }.toIterator
    }

    computedBatches.flatten
  }

  /**
   * Releases any resources taken up by the handler
   */
  def close() {
    metricProvider(GeocodingHandlerMetric.totalCount(totalRowsCodedCounter.get()))
    metricProvider(GeocodingHandlerMetric.failureCount(noMatchRowsCounter.get()))
    metricProvider(GeocodingHandlerMetric.milliseconds(timeCounter.get()))

    // $COVERAGE-OFF$ This is only logging, we don't need to test it.
    logger.info(s"${totalRowsCodedCounter.get()} row(s) geocoded in ${timeCounter.get()} milliseconds")
    if (noMatchRowsCounter.get() > 0) {
      logger.info(s"${noMatchRowsCounter.get()} row(s) failed to geocode to a point")
    }
    // $COVERAGE-ON$
  }

  // Address components
  private val address = "address"
  private val city = "city"
  private val state = "state"
  private val zip = "zip"
  private val country = "country"

  /**
   * Retrieves the ids of the columns containing address, city, state, zip, country
   * defined in the computed column parameters as 'address', 'city', 'state', 'zip', 'country'
   * and listed as source columns in the computation strategy
   * @param computedColumn Computed column definition
   * @return               Map from address component to some column id if it is defined
   */
  private def getColumnIds(computedColumn: ColumnRecordLike): Map[String, Option[ColumnId]] = {
    val fieldNames = getColumnNames(computedColumn)
    computedColumn.computationStrategy match {
      case Some(ComputationStrategyRecord(_, _, Some(sourceColumns), _)) =>
        fieldNames.mapValues {
          case Some(name) =>
            sourceColumns.find(_.fieldName == name) match {
              case Some(column) => Some(column.id)
              case None => logger.info(s"Source column $name not defined in computation strategy"); None
            }
          case None => None
        }
      case _ => throw new GeocodingHandler.InvalidComputationStrategyException()
    }
  }

  /**
   * Retrieves the field names of the columns containing address, city, state, zip, country
   * defined in the computed column parameters as 'address', 'city', 'state', 'zip', 'country'
   * @param computedColumn Computed column definition
   * @return               Map from address component to column field name
   */
  private def getColumnNames(computedColumn: ColumnRecordLike): Map[String, Option[ColumnName]] = {
    require(computedColumn.computationStrategy.isDefined, "No computation strategy found")
    computedColumn.computationStrategy match {
      case Some(ComputationStrategyRecord(_, _, _, Some(JObject(params)))) =>
        Map(
          address -> getColumnName(address, params),
          city -> getColumnName(city, params),
          state -> getColumnName(state, params),
          zip -> getColumnName(zip, params),
          country -> getColumnName(country, params)
        )
      case _ => throw new GeocodingHandler.InvalidComputationStrategyParamsException()
    }
  }

  private def getColumnName(field: String, params: sc.Map[String, JValue]): Option[ColumnName] = {
    params.get(field) match {
      case Some(JString(name)) => Some(ColumnName(name))
      case Some(JNull)         => None
      case Some(other)         => throw new IllegalArgumentException(s"'$field' value '$other' in parameters is not a string")
      case None                => None
    }
  }

  /**
   * Extracts the full address given by the source address, city, state, zip columns
   * @param rowmap Map of fields in the row
   * @param colIds Ids of the address, city, state, zip columns
   * @return       Combined value of the source columns as an Address(address, city, state, zip)
   */
  private def extractAddress(rowmap: SoQLRow, colIds: Map[String, Option[ColumnId]]): Address = {
    val values = extractSourceColumnValuesFromRow(rowmap, colIds)
    Address(values(address), values(city), values(state), values(zip), values(country)) // country defaults to US
  }

  private def extractSourceColumnValuesFromRow(rowmap: SoQLRow, colIds: Map[String, Option[ColumnId]]): Map[String, Option[String]] = {
    colIds.map({
      case (field, Some(colId)) => (field, rowmap.get(colId.underlying) match {
        case Some(SoQLText(str))   => Some(str)
        case Some(SoQLNumber(num)) => if (field == zip) Some(num.toString) // zip codes could be a number sadly...
                                      else throw MaltypedDataEx(ColumnName(colId.underlying), SoQLText, SoQLNumber)
        case Some(SoQLNull) | None => None
        case Some(other)           => throw MaltypedDataEx(ColumnName(colId.underlying), SoQLText, other.typ)
      })
      case (field, None) => (field, None) // not all parts of the address need to be provided
    })
  }

  /**
   * Geocodes a batch of addresses to points
   * @param addresses Sequence of addresses to geocode
   * @return          Sequence of results; a result will be Some(Point) if the address
   *                  can be meaningfully geocoded, else the result will be None
   */
  private def geocode(requestId: RequestId, addresses: Seq[Address]): Seq[Option[Point]] = {
    try {
      geocoder.geocode(addresses).map(toPoint)
    } catch {
      case e : Throwable =>
        logger.error(s"Geocoding failed: ${e.getMessage}")
        addresses.map(_ => None) // don't stop ingress due to geocoding failure
    }
  }

  private val geometryFactory = new GeometryFactory()

  private def toPoint(latLonOpt: Option[LatLon]): Option[Point] = {
    latLonOpt match {
      case Some(LatLon(lat, lon)) => Some(geometryFactory.createPoint(new Coordinate(lat, lon)))
      case None => None
    }
  }

}

object GeocodingHandler {
  class InvalidComputationStrategyException extends IllegalArgumentException("Invalid computation strategy")

  class InvalidComputationStrategyParamsException extends IllegalArgumentException(
    "Computation strategy parameters were invalid. " + """Expected format: {
                                                         |"address" : "[ADDRESS_FIELD_NAME]",
                                                         |"city" : "[CITY_FIELD_NAME]",
                                                         |"state" : "[STATE_FIELD_NAME]",
                                                         |"zip" : "[ZIP_FIELD_NAME]",
                                                         |"country" : "[COUNTRY_FIELD_NAME]"}""".stripMargin
  )
}
