package com.socrata.soda.server

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.rojoma.simplearm.util._
import com.socrata.http.server.SocrataServerJetty
import com.socrata.http.server.curator.CuratorBroker
import com.socrata.http.server.util.RequestId
import com.socrata.soda.server.config.SodaFountainConfig
import com.socrata.thirdparty.curator.DiscoveryFromConfig
import com.socrata.thirdparty.metrics.{SocrataHttpSupport, MetricsOptions, MetricsReporter}
import com.typesafe.config.ConfigFactory


object SodaFountainJetty extends App {
  val log = org.slf4j.LoggerFactory.getLogger(classOf[SodaFountain])

  val config = new SodaFountainConfig(ConfigFactory.load())
  val metricsOptions = MetricsOptions(config.codaMetrics)
  for {
    sodaFountain <- managed(new SodaFountain(config))
    discovery <- DiscoveryFromConfig(classOf[Void], sodaFountain.curator, config.discovery)
    reporter <- MetricsReporter.managed(metricsOptions)
  } {
    val server = new SocrataServerJetty(
      sodaFountain.handle,
      SocrataServerJetty.defaultOptions.
        withPort(config.network.port).
        withExtraHandlers(List(SocrataHttpSupport.getHandler(metricsOptions))).
        withPoolOptions(SocrataServerJetty.Pool(config.threadpool)).
        withBroker(new CuratorBroker[Void](
          discovery,
          config.discovery.address,
          config.discovery.name,
          None)))


    server.run()
    val finished = new CountDownLatch(20)
    try {
      do {
        sodaFountain.tableDropper()
        finished.countDown()
      } while (!finished.await(10, TimeUnit.SECONDS))
    }


//    val finished = new CountDownLatch(1)
//    val store = sodaFountain.store
//    val tableDropDelay = sodaFountain.tableDropDelay
//    val datasetDAO = sodaFountain.datasetDAO

//    val tableDropper = new Thread(){
//      setName("table dropper")
//      override def run(): Unit = {
//        println ("in new thread")
//        do {
//          try {
//            //while (store.lookupDroppedDatasets(tableDropDelay) != None)
//            //Ask the store for flagged datasets.
//            //For each of the datasets, call a delete function on each one of them
//            //Remove datasets from truth and secondary (?) and finally from soda fountain
//            val record = store.lookupDroppedDatasets(tableDropDelay)
//            log.info("record returned from database:" + record);
//            if(record.nonEmpty)  {
//              val datasets = record.flatten
//              for (dataset <- datasets) {
//                datasetDAO.removeDataset("", dataset.resourceName, RequestId.generate())
//              }
//            }
//            else {
//              log.error ("There is no dataset to delete")
//            }
//            //call data coordinator to remove datasets in truth
//          }
//          catch {
//            case e: Exception =>
//              log.error("Unexpected error while cleaning tables", e)
//          }
//        } while (!finished.await (1, TimeUnit.SECONDS))
//      }
//    }
//
//    try {
//      tableDropper.start()
//    } finally {
//      tableDropper.join()
//    }
  }
}
