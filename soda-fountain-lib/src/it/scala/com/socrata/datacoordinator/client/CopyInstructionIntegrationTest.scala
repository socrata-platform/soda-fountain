package com.socrata.datacoordinator.client

import dispatch._
import scala.concurrent.ExecutionContext.Implicits.global

class CopyInstructionIntegrationTest extends DataCoordinatorIntegrationTest {

  val userName = "daniel_the_tester"

  test("Mutation Script can create/delete dataset"){

    val responses = for {
      idAndHash <-fountain.dc.create( "it_test_publish", userName, None).right
      delete <- fountain.dc.deleteAllCopies(idAndHash._1, idAndHash._2,userName).right
    } yield (idAndHash, delete)

    responses() match {
      case Right((idAndHash, delete)) => {
        delete.getResponseBody must equal ("""[]""".stripMargin)
      }
      case Left(thr) => throw thr
    }
  }

  test("Mutation Script can copy/publish dataset"){

    val responses = for {
      idAndHash <-fountain.dc.create( "it_test_publish", userName, None).right
      copy <- fountain.dc.copy(idAndHash._1, idAndHash._2, false, userName, None).right
      publish <- fountain.dc.publish(idAndHash._1, idAndHash._2, None, userName, None).right
    } yield (idAndHash, copy, publish)

    responses() match {
      case Right((idAndHash, copy, publish)) => {
        copy.getResponseBody must equal ("""[]""".stripMargin)
        publish.getResponseBody must equal ("""[]""".stripMargin)
      }
      case Left(thr) => throw thr
    }
  }


  test("Mutation Script can copy/drop dataset"){

    val responses = for {
      idAndHash <-fountain.dc.create( "it_test_drop", userName, None).right
      copy <- fountain.dc.copy(idAndHash._1, idAndHash._2, false, userName, None).right
      drop <- fountain.dc.dropCopy(idAndHash._1, idAndHash._2, userName, None).right
    } yield (idAndHash, copy, drop)

    responses() match {
      case Right((idAndHash, copy, drop)) => {
        copy.getResponseBody must equal ("""[]""".stripMargin)
        drop.getResponseBody must equal ("""[]""".stripMargin)
      }
      case Left(thr) => throw thr
    }
  }
}