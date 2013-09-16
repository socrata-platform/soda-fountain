package com.socrata.soda.server.mocks

import com.socrata.datacoordinator.client.DataCoordinatorClient
import com.socrata.soda.server.services.SodaService
import com.socrata.http.client.{HttpClientHttpClient, InetLivenessChecker}
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent._
import java.util.concurrent.TimeUnit._
import com.socrata.InternalHttpClient


trait LocalDataCoordinator extends SodaService {

  val dc: DataCoordinatorClient = mock

  private object mock extends DataCoordinatorClient with InternalHttpClient {
    def hostO = Some("localhost:12345")
  }
}
