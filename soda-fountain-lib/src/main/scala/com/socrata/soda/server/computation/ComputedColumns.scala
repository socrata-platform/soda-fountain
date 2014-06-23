package com.socrata.soda.server.computation

import com.socrata.soda.server.persistence._
import com.socrata.soql.types.SoQLValue
import com.socrata.soda.server.wiremodels.ComputationStrategyType

/**
 * Utilities for computing columns
 */
object ComputedColumns {
  type SoQLRow = collection.immutable.Map[String, SoQLValue]

  sealed trait ComputeResult
  case class ComputeSuccess(it: Iterator[SoQLRow]) extends ComputeResult
  case class HandlerNotFound(typ: ComputationStrategyType.Value) extends ComputeResult

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
   * @param sourceIt an Iterator of source rows, each row is a JValue (actually an JObject
   *                 of key-value pairs, where the key is the column name).  Anything other
   *                 than a JObject is not for upserts and can be ignored.
   * @param computedColumns the list of computed columns from [[findComputedColumns]]
   */
  def addComputedColumns(sourceIt: Iterator[SoQLRow],
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

  def handlers = Map[ComputationStrategyType.Value, () => ComputationHandler](
    ComputationStrategyType.GeoRegion -> (() => new GeospaceHandler),
    ComputationStrategyType.Test      -> (() => new TestComputationHandler)
  )
}