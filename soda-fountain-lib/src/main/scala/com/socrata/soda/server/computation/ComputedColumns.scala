package com.socrata.soda.server.computation

import com.socrata.http.client.HttpClient
import com.socrata.soda.server.wiremodels.ComputationStrategyType
import com.typesafe.config.Config
import org.apache.curator.x.discovery.ServiceDiscovery

/**
 * Utilities for computing columns.  Also holds state for computation handlers and initializing them.
 *
 * @param handlersConfig a Typesafe Config containing an entry for configuring each handler.
 * @param discovery the ServiceDiscovery instance used for discovering other services using ZK/Curator
 */
class ComputedColumns[T](handlersConfig: Config, discovery: ServiceDiscovery[T], http: HttpClient) extends ComputedColumnsLike {

  /**
   * Instantiates a computation handler handle a given computation strategy type.
   */
  val handlers = Map[ComputationStrategyType.Value, () => ComputationHandler](
    ComputationStrategyType.GeoRegionMatchOnPoint  -> (() => geoRegionMatchOnPointHandler),
    ComputationStrategyType.GeoRegionMatchOnString -> (() => geoRegionMatchOnStringHandler),
    ComputationStrategyType.Test                   -> (() => new TestComputationHandler),

    // For backwards compatibility only. Replaced by GeoRegionMatchOnPoint
    ComputationStrategyType.GeoRegion              -> (() => geoRegionMatchOnPointHandler)
  )

  private def geoRegionMatchOnPointHandler  = new GeoregionMatchOnPointHandler(
    handlersConfig.getConfig("region-coder"), discovery, http)
  private def geoRegionMatchOnStringHandler = new GeoregionMatchOnStringHandler(
    handlersConfig.getConfig("region-coder"), discovery, http)
}
