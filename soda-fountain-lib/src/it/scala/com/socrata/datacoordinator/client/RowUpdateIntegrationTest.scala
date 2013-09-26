package com.socrata.datacoordinator.client

import com.socrata.soda.clients.datacoordinator.RowUpdateOptionChange

class RowUpdateIntegrationTest extends DataCoordinatorIntegrationTest {

  test("can declare row data"){
    val idAndResults = dc.create(instance, userName, None)
    dc.update(idAndResults._1, mockSchemaString, userName, Array(RowUpdateOptionChange(true, false, true)).iterator){rowDataDecResp =>
      //rowDataDec.getResponseBody must equal ("""[{"inserted":{},"updated":{},"deleted":{},"errors":{}}]""".stripMargin)
    }
  }
}