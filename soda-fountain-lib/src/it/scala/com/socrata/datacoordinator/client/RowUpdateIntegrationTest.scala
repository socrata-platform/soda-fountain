package com.socrata.datacoordinator.client

import dispatch._
import scala.concurrent.ExecutionContext.Implicits.global

class RowUpdateIntegrationTest extends DataCoordinatorIntegrationTest {

  val userName = "daniel_the_tester"

  test("can declare row data"){
    val responses = for {
      idAndResults <-fountain.dc.create(userName, None).right
      rowDataDec <- fountain.dc.update(idAndResults._1, None, userName, Array(RowUpdateOptionChange(true, false, true)).iterator).right
    } yield (idAndResults, rowDataDec)

    pendingUntilFixed{
      responses() match {
        case Right((idAndResults, rowDataDec)) => {
          rowDataDec.getResponseBody must equal ("""[{"inserted":{},"updated":{},"deleted":{},"errors":{}}]""".stripMargin)
        }
        case Left(thr) => throw thr
      }
    }
  }
}