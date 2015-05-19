package com.socrata.soda.server.computation

import java.util.concurrent.TimeUnit

import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.codec._
import com.rojoma.json.v3.io.{CompactJsonWriter, JsonReader}
import com.socrata.soda.server.highlevel.RowDataTranslator
import com.socrata.soda.server.highlevel.RowDataTranslator._
import com.socrata.soda.server.metrics.MetricCounter
import com.socrata.soda.server.persistence.{ComputationStrategyRecord, ColumnRecordLike}
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types.SoQLNumber
import com.socrata.thirdparty.curator.CuratorServiceBase
import com.typesafe.config.Config
import org.apache.curator.x.discovery.ServiceDiscovery
import org.slf4j.LoggerFactory
import scala.annotation.tailrec
import scalaj.http.{HttpOptions, Http}

/**
 * A [[ComputationHandler]] that uses region-coder service to match a row to a georegion,
 * based on the value of a specified source column.
 * @param config    Configuration information for connecting to region-coder service
 * @param discovery ServiceDiscovery instance used for discovering other services using ZK/Curator
 * @tparam T        ServiceDiscovery payload type
 * @tparam V        Type of the source column passed to region-coder service to get back a matching georegion
 */
abstract class GeoregionMatchHandler[T, V](config: Config, discovery: ServiceDiscovery[T]) extends ComputationHandler {
  import ComputationHandler._

  // Get config values
  val serviceName    = config.getString("service-name")
  val batchSize      = config.getInt("batch-size")
  val maxRetries     = config.getInt("max-retries")
  val retryWait      = config.getDuration("retry-wait", TimeUnit.MILLISECONDS)
  val connectTimeout = config.getDuration("connect-timeout", TimeUnit.MILLISECONDS).toInt
  val readTimeout    = config.getDuration("read-timeout", TimeUnit.MILLISECONDS).toInt

  class RegionCoderService[T](discovery: ServiceDiscovery[T]) extends CuratorServiceBase(discovery, serviceName)
  val service = new RegionCoderService(discovery)
  service.start()

  def urlPrefix = Option(service.provider.getInstance()).map { serv => serv.buildUriSpec() + "v1" }.
    getOrElse(throw new RuntimeException("Unable to get region-coder instance from Curator/ZK"))

  private val logger = LoggerFactory.getLogger(getClass)

  private val totalRowsCodedCounter = new MetricCounter()
  private val noMatchRowsCounter = new MetricCounter()
  private val timeCounter = new MetricCounter()

  /**
   * Constructs the region-coder endpoint
   * @param computedColumn Computed column definition
   * @return               Endpoint to be used for region coding
   */
  protected def genEndpoint(computedColumn: ColumnRecordLike): String

  /**
   * Extracts the source column value given the key-value map of fields in the row
   * @param rowmap  Map of fields in the row
   * @param colName Name of the source column to extract
   * @return        Value of the source column
   */
  protected def extractSourceColumnValueFromRow(rowmap: SoQLRow, colName: ColumnName): Option[V]

  /**
   * Converts the source column value to a JSON format that region-coder understands
   * @param value Raw value of the source column
   * @return      Source column value in the format expected by region-coder
   */
  protected def toJValue(value: V): JValue

  /**
   * A single-threaded (for now) geo-region-coding handler.  Batches and sends out the source
   * column values to region-coder, then incorporates the feature IDs into a new column.
   * @param sourceIt Iterator of rows to be georegion coded
   * @param column   Computed column definition
   * @return         The original set of rows with the feature ID of the matching georegion appended to each row
   */
  def compute(sourceIt: Iterator[RowDataTranslator.Computable], column: ColumnRecordLike): Iterator[RowDataTranslator.Computable] = {
    require(column.computationStrategy.isDefined, "No computation strategy found")

    // Only a single column is allowed as a source for now
    val sourceColumnId = extractSourceColumnId(column)

    val batches = sourceIt.grouped(batchSize)
    val computedBatches = batches.map { batch =>
      val rowsWithIndex = batch.zipWithIndex.toSeq

      // Grab just the upserts and get the source column values for mapping to feature ID
      val sourceValuesWithIndex = rowsWithIndex.collect {
        case (upsert: UpsertAsSoQL, i) => (extractSourceColumnValueFromRow(upsert.rowData.toMap, ColumnName(sourceColumnId)), i)
      }.collect {
        case (Some(point), i)          => (point, i)
      }

      // Convert points to feature IDs, and splice feature IDs back into rows.
      // Deletes are returned untouched.
      val start = System.currentTimeMillis
      val endpoint = genEndpoint(column)
      val featureIds = regionCoder(endpoint, sourceValuesWithIndex.map(_._1))
      timeCounter.add(System.currentTimeMillis - start)
      noMatchRowsCounter.add(featureIds.count(_.isEmpty))
      val featureIdsWithIndex = sourceValuesWithIndex.map(_._2).zip(featureIds).toMap
      rowsWithIndex.map {
        case (upsert: UpsertAsSoQL, i) =>
          totalRowsCodedCounter.increment()
          featureIdsWithIndex.get(i).flatMap { maybeFeatureId =>
            maybeFeatureId.map { featureId =>
              UpsertAsSoQL(upsert.rowData + (column.id.underlying -> SoQLNumber(java.math.BigDecimal.valueOf(featureId))))
            }
          }.getOrElse(upsert)
        case (delete: DeleteAsCJson, i) => delete
        case _                     =>
          val message = s"Unsupported row update type passed into ${getClass.getSimpleName}"
          logger.error(message)
          throw ComputationEx(message, None)
      }.toIterator
    }

    computedBatches.flatten
  }

  def close() {
    // TODO : Hook this up to Balboa
    // $COVERAGE-OFF$ This is only logging, we don't need to test it.
    logger.info(s"${totalRowsCodedCounter.get()} row(s) georegion coded in ${timeCounter.get()} milliseconds")
    if (noMatchRowsCounter.get() > 0) {
      logger.info(s"${noMatchRowsCounter.get()} row(s) did not match any georegion")
    }
    // $COVERAGE-ON$

    logger.info(s"Closing ${getClass.getSimpleName}...")
    service.close()
  }

  implicit def optionIntCodec = new JsonEncode[Option[Int]] with JsonDecode[Option[Int]] {
    def encode(x: Option[Int]) = x match {
      case Some(value) => JsonEncode[Int].encode(value)
      case None        => com.rojoma.json.v3.ast.JNull
    }

    def decode(x: JValue): JsonDecode.DecodeResult[Option[Int]] =
      Right(JsonDecode[Int].decode(x).right.toOption)
  }

  private def extractSourceColumnId(column: ColumnRecordLike): String = {
    val sourceCol = for {
      strategy   <- column.computationStrategy
      sourceCols <- strategy.sourceColumns
      first      <- sourceCols.headOption
    } yield first

    sourceCol.map(_.id.underlying).getOrElse(
      throw new IllegalArgumentException("Source column was not defined in computation strategy"))
  }

  private def regionCoder(endpoint: String, items: Seq[V]): Seq[Option[Int]] = {
    if (items.size == 0) return Seq.empty[Option[Int]]

    val url = urlPrefix + endpoint
    logger.debug("HTTP POST [{}] with {} items...", url, items.length)
    val (status, response) = postWithRetry(url, JArray(items.map(toJValue)), maxRetries)

    logger.debug("Got back status {}, response [{}]", status, response)
    status match {
      case 200 =>
        JsonDecode[Seq[Option[Int]]].decode(JsonReader.fromString(response)).right.
          getOrElse(throw ComputationEx("Error parsing JSON response: " + response, None))
      case sc  =>
        val errorMessage = s"Error: HTTP [$url] got response code $sc, body $response"
        logger.error(errorMessage)
        throw ComputationEx(errorMessage, None)
    }
  }

  // Note: "status" cannot be a 4xx or 5xx, because that will throw a HttpException
  @tailrec
  private def postWithRetry(url: String, payload: JArray, retriesLeft: Int): (Int, String) = {
    try {
      val (status, _, response) = Http.postData(url, CompactJsonWriter.toString(payload)).
        header("content-type", "application/json").
        option(HttpOptions.connTimeout(connectTimeout)).
        option(HttpOptions.readTimeout(readTimeout)).
        asHeadersAndParse(Http.readString)
      (status, response)
    } catch {
      case e: scalaj.http.HttpException =>
        if (retriesLeft > 0) {
          Thread.sleep(retryWait)
          postWithRetry(url, payload, retriesLeft - 1)
        } else {
          logger.error("HTTP Error: ", e)
          throw ComputationEx("HTTP Error reading " + url, Some(e))
        }
    }
  }
}
