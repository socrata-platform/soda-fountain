package com.socrata.soda.server.computation

import com.socrata.computation_strategies.StrategyType
import com.socrata.soda.server.id.ResourceName

import com.typesafe.config.Config

import org.apache.curator.x.discovery.ServiceDiscovery

/**
 * Utilities for computing columns.  Also holds state for computation handlers and initializing them.
 *
 * @param handlersConfig a Typesafe Config containing an entry for configuring each handler.
 * @param discovery the ServiceDiscovery instance used for discovering other services using ZK/Curator
 */
class ComputedColumns[T](handlersConfig: Config, discovery: ServiceDiscovery[T], computingGate: ResourceName => Boolean) extends ComputedColumnsLike {

  /**
   * Instantiates a computation handler handle a given computation strategy type.
   */
  val handlers = Map[StrategyType, () => ComputationHandler](
    StrategyType.GeoRegionMatchOnPoint  -> (() => geoRegionMatchOnPointHandler),
    StrategyType.GeoRegionMatchOnString -> (() => geoRegionMatchOnStringHandler),
    StrategyType.Test                   -> (() => new TestComputationHandler),

    // For backwards compatibility only. Replaced by GeoRegionMatchOnPoint
    StrategyType.GeoRegion              -> (() => geoRegionMatchOnPointHandler)
  )

  private def geoRegionMatchOnPointHandler  = new GeoregionMatchOnPointHandler(
    handlersConfig.getConfig("region-coder"), discovery)
  private def geoRegionMatchOnStringHandler = new GeoregionMatchOnStringHandler(
    handlersConfig.getConfig("region-coder"), discovery)

  def computingEnabled(resourceName: ResourceName): Boolean = computingGate(resourceName)
}
