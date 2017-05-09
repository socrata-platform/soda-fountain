package com.socrata.soda.server

import com.socrata.computation_strategies.StrategyType
import com.socrata.soda.server.computation.{ComputationHandler, ComputedColumnsLike, TestComputationHandler}
import com.socrata.soda.server.id.ResourceName

object TestComputedColumns extends ComputedColumnsLike {
  val handlers = Map[StrategyType, () => ComputationHandler](
    StrategyType.Test      -> (() => new TestComputationHandler)
  )
  def computingEnabled(resourceName: ResourceName) = true
}
