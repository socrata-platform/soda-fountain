package com.socrata.soda.server.computation

import com.rojoma.json.ast.JObject
import com.socrata.soda.server.persistence._
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types._

/**
 *  Defines a handler capable of computing one type of computed column
 */
trait ComputationHandler {
  // Use an immutable map to guarantee no mutation for safe concurrency
  type SoQLRow = collection.immutable.Map[String, SoQLValue]

  /**
   * Handles the actual computation.  Must be lazy, otherwise will introduce significant latency
   * and OOM errors for large upserts.  IE, try not to convert the Iterator to a regular Seq or Array.
   *
   * @param sourceIt an Iterator of source rows, each row is a Map[String, SoQLValue], where the key is the
   *                 fieldName and the value is a SoQLValue representation of source data
   * @param column a ColumnRecord describing the computation and parameters
   * @return an Iterator[SoQLRow] for the output rows.  One of the keys must containing the output column.
   */
  def compute(sourceIt: Iterator[SoQLRow], column: MinimalColumnRecord): Iterator[SoQLRow]
}

object ComputationHandler {
  // TODO: These are repeated from RowDAOImpl.  Let's share these somehow.  Or maybe we just make them private.
  case class UnknownColumnEx(colName: ColumnName) extends Exception
  case class MaltypedDataEx(col: ColumnName, expected: SoQLType, got: SoQLType) extends Exception
  case class ComputationEx(message: String, underlying: Option[Throwable]) extends Exception
}
