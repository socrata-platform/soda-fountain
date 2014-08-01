package com.socrata.soda.server.computation

import com.socrata.soda.server.highlevel.RowDataTranslator
import com.socrata.soda.server.persistence._
import com.socrata.soda.server.util.ManagedIterator
import com.socrata.soda.server.wiremodels.ComputationStrategyType
import com.typesafe.config.Config
import org.apache.curator.x.discovery.ServiceDiscovery

object ComputedColumns {
  sealed trait ComputeResult
  case class ComputeSuccess(it: Iterator[RowDataTranslator.Computable]) extends ComputeResult
  case class HandlerNotFound(typ: ComputationStrategyType.Value) extends ComputeResult
}

/**
 * Utilities for computing columns.  Also holds state for computation handlers and initializing them.
 *
 * @param handlersConfig a Typesafe Config containing an entry for configuring each handler.
 * @param discovery the ServiceDiscovery instance used for discovering other services using ZK/Curator
 */
class ComputedColumns[T](handlersConfig: Config, discovery: ServiceDiscovery[T]) {
  import ComputedColumns._

  /**
   * Instantiates a computation handler handle a given computation strategy type.
   */
  val handlers = Map[ComputationStrategyType.Value, () => ComputationHandler](
    ComputationStrategyType.GeoRegion -> (() => new GeospaceHandler(handlersConfig.getConfig("geospace"),
                                                                    discovery)),
    ComputationStrategyType.Test      -> (() => new TestComputationHandler)
  )

  /**
   * Finds the computed columns from the dataset schema.
   *
   * @param datasetRecord containing the schema of the dataset
   * @return a Seq[ColumnRecord] containing all the columns described in the dataset with a computationStrategy
   */
  def findComputedColumns(datasetRecord: DatasetRecordLike): Seq[ColumnRecordLike] =
    datasetRecord.columns.filter { col => col.computationStrategy.isDefined }

  /**
   * Performs the (hopefully lazy) computation of all computed columns, producing a new iterator with
   * the new computed columns filled in.   The computations are chained, one computed column at a time;
   * thus if all computation handlers are lazy then the processing can happen incrementally.
   * NOTE: there is no way currently to know if an incoming dataset actually _needs_ all the computations
   * to be done.
   * @param sourceIt an Iterator of row updates, includes both upsert and deletes.
   *                 For upserts, each row is a Map[String, SoQLValue], where the key is the
   *                 fieldName and the value is a SoQLValue representation of source data.
   *                 Deletes contain only row PK and can be ignored.
   * @param computedColumns the list of computed columns from [[findComputedColumns]]
   */
  def addComputedColumns(sourceIt: Iterator[RowDataTranslator.Computable],
                         computedColumns: Seq[ColumnRecordLike]): ComputeResult = {
    var rowIterator = sourceIt
    for (computedColumn <- computedColumns) {
      val tryGetHandler = handlers.get(computedColumn.computationStrategy.get.strategyType)
      tryGetHandler match {
        case Some(handlerCreator) =>
          val handler = handlerCreator()
          rowIterator = new ManagedIterator(handler.compute(rowIterator, computedColumn), handler)
        case None =>
          return HandlerNotFound(computedColumn.computationStrategy.get.strategyType)
      }
    }
    ComputeSuccess(rowIterator)
  }
}