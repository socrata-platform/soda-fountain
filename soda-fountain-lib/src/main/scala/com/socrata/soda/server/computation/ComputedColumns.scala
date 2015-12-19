package com.socrata.soda.server.computation

import com.socrata.soda.server.metrics.Metrics.Metric
import com.socrata.soda.server.wiremodels.ComputationStrategyType
import com.socrata.geocoders.Geocoder
import com.typesafe.config.Config
import org.apache.curator.x.discovery.ServiceDiscovery

/**
 * Utilities for computing columns.  Also holds state for computation handlers and initializing them.
 *
 * @param handlersConfig a Typesafe Config containing an entry for configuring each handler.
 * @param discovery the ServiceDiscovery instance used for discovering other services using ZK/Curator
 */
class ComputedColumns[T](handlersConfig: Config, discovery: ServiceDiscovery[T], geocoder: Geocoder, metricProvider: (Metric => Unit)) extends ComputedColumnsLike {

  /**
   * Instantiates a computation handler to handle a given computation strategy type.
   */
  val handlers = Map[ComputationStrategyType.Value, () => ComputationHandler](
    ComputationStrategyType.GeoRegionMatchOnPoint  -> (() => geoRegionMatchOnPointHandler),
    ComputationStrategyType.GeoRegionMatchOnString -> (() => geoRegionMatchOnStringHandler),
    ComputationStrategyType.GeoCoding              -> (() => geoCodingHandler),
    ComputationStrategyType.Test                   -> (() => new TestComputationHandler),

    // For backwards compatibility only. Replaced by GeoRegionMatchOnPoint
    ComputationStrategyType.GeoRegion              -> (() => geoRegionMatchOnPointHandler)
  )

  private def geoRegionMatchOnPointHandler  = new GeoregionMatchOnPointHandler(
    handlersConfig.getConfig("region-coder"), discovery, metricProvider)
  private def geoRegionMatchOnStringHandler = new GeoregionMatchOnStringHandler(
    handlersConfig.getConfig("region-coder"), discovery, metricProvider)
  private def geoCodingHandler = new GeocodingHandler(
    handlersConfig.getConfig("geo-coder"), geocoder, metricProvider)
}