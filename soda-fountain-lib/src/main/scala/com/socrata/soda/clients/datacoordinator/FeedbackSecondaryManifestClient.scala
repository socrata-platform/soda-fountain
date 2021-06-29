package com.socrata.soda.clients.datacoordinator

import com.socrata.computation_strategies.StrategyType
import com.socrata.soda.server.id.{DatasetHandle, SecondaryId}

class FeedbackSecondaryManifestClient(dc: DataCoordinatorClient,
                                      feedbackSecondaryIdMap: Map[StrategyType, SecondaryId]) {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[FeedbackSecondaryManifestClient])

  def maybeReplicate(datasetId: DatasetHandle,
                     strategyTypes: Set[StrategyType]): Unit = {
    strategyTypes.flatMap { typ => feedbackSecondaryIdMap.get(typ) }.foreach { secondaryId =>
      try {
        dc.propagateToSecondary(datasetId, secondaryId, None)
        log.info(s"Added dataset ${datasetId.toString} to secondary manifest for feedback secondary {}",
          secondaryId.toString)
      } catch {
        case error: Exception =>
          // TODO: DataCoordinatorClient.propagateToSecondary(.) really should have better error handling...
          log.error("Failed to add dataset {} to secondary manifest for feedback secondary {}: {}",
            datasetId.toString, secondaryId.toString, error)
      }
    }
  }
}
