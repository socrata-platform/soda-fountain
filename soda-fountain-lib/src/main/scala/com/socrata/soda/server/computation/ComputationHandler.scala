package com.socrata.soda.server.computation

import com.socrata.http.server.util.RequestId.RequestId
import com.socrata.soda.server.{SodaInternalException, SodaInvalidRequestException}
import com.socrata.soda.server.highlevel.RowDataTranslator
import com.socrata.soda.server.persistence._
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types._

/**
 *  Defines a handler capable of computing one type of computed column
 */
trait ComputationHandler extends java.io.Closeable {
  // Use an immutable map to guarantee no mutation for safe concurrency
  type SoQLRow = collection.immutable.Map[String, SoQLValue]

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
              column: ColumnRecordLike): Iterator[RowDataTranslator.Computable]

  /**
   * Releases any resources taken up by the handler
   */
  def close()
}

object ComputationHandler {
  case class UnknownColumnEx(colName: ColumnName) extends SodaInvalidRequestException(s"Unknown column $colName")
  case class MaltypedDataEx(col: ColumnName, expected: SoQLType, got: SoQLType) extends SodaInvalidRequestException(s"Invalid data for column $col; expected a $expected and got a $got")
  case class ComputationEx(message: String, underlying: Option[Throwable]) extends SodaInternalException(message, underlying.orNull)
}
