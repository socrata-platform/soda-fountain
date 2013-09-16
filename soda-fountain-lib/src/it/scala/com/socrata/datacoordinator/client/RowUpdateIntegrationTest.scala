package com.socrata.datacoordinator.client

import dispatch._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Success, Failure}

class RowUpdateIntegrationTest extends DataCoordinatorIntegrationTest {

  val userName = "daniel_the_tester"

  test("can declare row data"){
    val responses = for {
      idAndResults <-fountain.dc.create(userName, None)
      rowDataDec <- fountain.dc.update(idAndResults._1, None, userName, Array(RowUpdateOptionChange(true, false, true)).iterator)
    } yield (idAndResults, rowDataDec)

    pendingUntilFixed{
      responses match {
        case Success((idAndResults, rowDataDec)) => {
          //rowDataDec.getResponseBody must equal ("""[{"inserted":{},"updated":{},"deleted":{},"errors":{}}]""".stripMargin)
        }
        case Failure(thr) => throw thr
      }
    }
  }
}