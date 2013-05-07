package com.socrata.datacoordinator.client

import dispatch._
import scala.concurrent.ExecutionContext.Implicits.global

class RowUpdateIntegrationTest extends DataCoordinatorIntegrationTest {

  val userName = "daniel_the_tester"

  test("can declare row data"){
    val responses = for {
      idAndHash <-fountain.dc.create("it_declare_row_data", userName, None).right
      rowDataDec <- fountain.dc.update(idAndHash._1, idAndHash._2, userName, Array(RowUpdateOptionChange(true, false, true)).toIterable).right
    } yield (idAndHash, rowDataDec)

    responses() match {
      case Right((idAndHash, rowDataDec)) => {
        rowDataDec.getResponseBody must equal ("""[]""".stripMargin)
      }
      case Left(thr) => throw thr
    }
  }
}