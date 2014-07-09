package com.socrata.soda.server.computation

import com.socrata.soda.server.highlevel.RowDataTranslator
import com.socrata.soda.server.persistence._
import com.socrata.soda.server.wiremodels.ComputationStrategyType
import com.socrata.soql.types.SoQLValue
import com.typesafe.config.ConfigFactory

/**
 * Utilities for computing columns
 */
object ComputedColumns {
  sealed trait ComputeResult
  case class ComputeSuccess(it: Iterator[RowDataTranslator.Success]) extends ComputeResult
  case class HandlerNotFound(typ: ComputationStrategyType.Value) extends ComputeResult

  val handlersConfig = ConfigFactory.load().getConfig("com.socrata.soda-fountain.handlers")

  /**
   * Instantiates a computation handler handle a given computation strategy type.
   */
  val handlers = Map[ComputationStrategyType.Value, () => ComputationHandler](
    ComputationStrategyType.GeoRegion -> (() => new GeospaceHandler(handlersConfig.getConfig("geospace"))),
    ComputationStrategyType.Test      -> (() => new TestComputationHandler)
  )

  /**
   * Finds the computed columns from the dataset schema.
   *
   * @param datasetRecord containing the schema of the dataset
   * @return a Seq[ColumnRecord] containing all the columns described in the dataset with a computationStrategy
   */
  def findComputedColumns(datasetRecord: MinimalDatasetRecord): Seq[MinimalColumnRecord] =
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
  def addComputedColumns(sourceIt: Iterator[RowDataTranslator.Success],
                         computedColumns: Seq[MinimalColumnRecord]): ComputeResult = {
    var rowIterator = sourceIt
    for (computedColumn <- computedColumns) {
      val tryGetHandler = handlers.get(computedColumn.computationStrategy.get.strategyType)
      tryGetHandler match {
        case Some(handler) => rowIterator = handler().compute(rowIterator, computedColumn)
        case None          => return HandlerNotFound(computedColumn.computationStrategy.get.strategyType)
      }
    }
    ComputeSuccess(rowIterator)
  }
}