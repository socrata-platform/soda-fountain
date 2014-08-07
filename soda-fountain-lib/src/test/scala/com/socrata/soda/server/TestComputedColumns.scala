package com.socrata.soda.server

import com.socrata.soda.server.computation.{TestComputationHandler, ComputationHandler, ComputedColumnsLike}
import com.socrata.soda.server.wiremodels.ComputationStrategyType

object TestComputedColumns extends ComputedColumnsLike {
  val handlers = Map[ComputationStrategyType.Value, () => ComputationHandler](
    ComputationStrategyType.Test      -> (() => new TestComputationHandler)
  )
}
