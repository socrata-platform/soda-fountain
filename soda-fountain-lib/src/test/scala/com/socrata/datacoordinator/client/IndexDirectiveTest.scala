package com.socrata.datacoordinator.client

import com.rojoma.json.v3.ast.{JBoolean, JObject}
import com.socrata.soda.clients.datacoordinator._
import com.socrata.soda.server.id.ColumnId

class IndexDirectiveTest extends DataCoordinatorClientTest {
  private val col = ColumnId("col")

  test("produce create or update mutation scription") {
    val addInst = CreateOrUpdateIndexDirectiveInstruction(col, JObject(Map("enabled" -> JBoolean.canonicalTrue)))
    addInst.toString must equal (normalizeWhitespace(s"""{"c":"create or update index directive","column":"col","directive":{"enabled":true}}"""))
  }


  test("produce drop mutation script") {
    val dropInst = new DropIndexDirectiveInstruction(col)
    dropInst.toString must equal (normalizeWhitespace(s"""{"c":"drop index directive","column":"col"}"""))
  }
}
