package com.socrata.datacoordinator.client

import scala.util.{Failure, Success}

class CopyInstructionIntegrationTest extends DataCoordinatorIntegrationTest {

  test("Mutation Script can create/delete dataset"){
    val idAndResults = dc.create(instance, userName, None)
    dc.deleteAllCopies(idAndResults._1, mockSchemaString,userName){deleteResponse =>
      idAndResults._1.underlying.length must be > (0)
      assertSuccess(deleteResponse)
    }
  }

  test("Mutation Script can copy/drop/publish dataset"){
    val idAndResults = dc.create(instance,  userName, None)
    dc.publish(idAndResults._1, mockSchemaString, None, userName, Iterator.empty){publishResult =>
      dc.copy(idAndResults._1, mockSchemaString, false, userName, Iterator.empty){copyResult =>
        dc.dropCopy(idAndResults._1, mockSchemaString, userName, Iterator.empty){dropResult =>
          dc.copy(idAndResults._1, mockSchemaString, false, userName, Iterator.empty){copyAgainResult =>
            dc.publish(idAndResults._1, mockSchemaString, None, userName, Iterator.empty){publishCopyResult =>
              //publish.getResponseBody must equal ("""[]""")
              //copy.getResponseBody must equal ("""[]""")
              //drop.getResponseBody must equal ("""[]""")
              //copyAgain.getResponseBody must equal ("""[]""")
              //publishCopy.getResponseBody must equal ("""[]""")
            }
          }
        }
      }
    }
  }
}