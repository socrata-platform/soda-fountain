package com.socrata.datacoordinator.client

import dispatch._
import scala.concurrent.ExecutionContext.Implicits.global

class CopyInstructionIntegrationTest extends DataCoordinatorIntegrationTest {

  val userName = "daniel_the_tester"

  test("Mutation Script can create/delete dataset"){

    val responses = for {
      idAndResults <-fountain.dc.create(userName, None).right
      delete <- fountain.dc.deleteAllCopies(idAndResults._1, None,userName).right
    } yield (idAndResults, delete)

    responses() match {
      case Right((idAndResults, delete)) => {
        idAndResults._1.length must be > (0)
        delete.getResponseBody must equal ("""[]""".stripMargin)
        println(idAndResults)
        delete.getStatusCode must equal (200)
      }
      case Left(thr) => throw thr
    }
  }

  test("Mutation Script can copy/drop/publish dataset"){

    val responses = for {
      idAndResults <-fountain.dc.create( userName, None).right
      publish <- fountain.dc.publish(idAndResults._1, None, None, userName, None).right
      copy <- fountain.dc.copy(idAndResults._1, None, false, userName, None).right
      drop <- fountain.dc.dropCopy(idAndResults._1, None, userName, None).right
      copyAgain <- fountain.dc.copy(idAndResults._1, None, false, userName, None).right
      publishCopy <- fountain.dc.publish(idAndResults._1, None, None, userName, None).right
    } yield (idAndResults, publish, copy, drop, copyAgain, publishCopy)

    responses() match {
      case Right((idAndResults, publish, copy, drop, copyAgain, publishCopy)) => {
        publish.getResponseBody must equal ("""[]""".stripMargin)
        copy.getResponseBody must equal ("""[]""".stripMargin)
        drop.getResponseBody must equal ("""[]""".stripMargin)
        copyAgain.getResponseBody must equal ("""[]""".stripMargin)
        publishCopy.getResponseBody must equal ("""[]""".stripMargin)
      }
      case Left(thr) => throw thr
    }
  }
}