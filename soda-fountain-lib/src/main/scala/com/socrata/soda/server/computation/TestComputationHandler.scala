package com.socrata.soda.server.computation

import com.socrata.soda.server.persistence.MinimalColumnRecord
import com.socrata.soql.types.SoQLText

class TestComputationHandler extends ComputationHandler {
  def compute(sourceIt: Iterator[SoQLRow], column: MinimalColumnRecord): Iterator[SoQLRow] = {
    require(column.computationStrategy.isDefined, "Computation strategy not defined")
    sourceIt.map(_ + (column.fieldName.name -> SoQLText("foo"))).toIterator
  }
}