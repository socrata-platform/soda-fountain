package com.socrata.soda.server.highlevel

import com.rojoma.json.ast.{JValue, JObject}
import com.socrata.soda.server.persistence._

/**
 * Utilities for computing columns
 */
object ComputedColumns {
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
   * @param handlers the set of ComputationHandlers available to fulfill computed column requests
   */
  def addComputedColumns(sourceIt: Iterator[JValue],  // TODO: make this SoQLROw...
                         computedColumns: Seq[MinimalColumnRecord],
                         handlers: Seq[ComputationHandler] = Nil): Iterator[JValue] = {
    var rowIterator = sourceIt
    for (computedColumn <- computedColumns;
         handler <- handlers.find(_.computationType == computedColumn.computationStrategy.get.strategyType.toString)) {
      // Now do something like this to chain:
      // rowIterator = handler.compute(rowIterator, computedColumn)
      //
      // Also handle the case where we cannot find a handler matching the given computation type
    }
    rowIterator
  }
}