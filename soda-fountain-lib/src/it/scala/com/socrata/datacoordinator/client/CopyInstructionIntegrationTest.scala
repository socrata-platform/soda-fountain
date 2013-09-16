package com.socrata.datacoordinator.client

import dispatch._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

class CopyInstructionIntegrationTest extends DataCoordinatorIntegrationTest {

  val userName = "daniel_the_tester"

  test("Mutation Script can create/delete dataset"){

    val responses = for {
      idAndResults <-fountain.dc.create(userName, None)
      delete <- fountain.dc.deleteAllCopies(idAndResults._1, None,userName)
    } yield (idAndResults, delete)

    responses match {
      case Success((datasetId, delete)) => {
        datasetId._1.underlying.length must be > (0)
        delete.length must be (0)
      }
      case Failure(thr) => throw thr
    }
  }

  test("Mutation Script can copy/drop/publish dataset"){

    val responses = for {
      idAndResults <-fountain.dc.create( userName, None)
      publish <- fountain.dc.publish(idAndResults._1, None, None, userName, None)
      copy <- fountain.dc.copy(idAndResults._1, None, false, userName, None)
      drop <- fountain.dc.dropCopy(idAndResults._1, None, userName, None)
      copyAgain <- fountain.dc.copy(idAndResults._1, None, false, userName, None)
      publishCopy <- fountain.dc.publish(idAndResults._1, None, None, userName, None)
    } yield (idAndResults, publish, copy, drop, copyAgain, publishCopy)

    responses match {
      case Success((idAndResults, publish, copy, drop, copyAgain, publishCopy)) => {
        //publish.getResponseBody must equal ("""[]""".stripMargin)
        //copy.getResponseBody must equal ("""[]""".stripMargin)
        //drop.getResponseBody must equal ("""[]""".stripMargin)
        //copyAgain.getResponseBody must equal ("""[]""".stripMargin)
        //publishCopy.getResponseBody must equal ("""[]""".stripMargin)
      }
      case Failure(thr) => throw thr
    }
  }
}