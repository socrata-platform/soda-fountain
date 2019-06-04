package com.socrata.datacoordinator.client

import com.rojoma.json.v3.ast.{JBoolean, JObject}
import com.socrata.soda.clients.datacoordinator._
import com.socrata.soql.environment.ColumnName

class SecondaryIndexTest extends DataCoordinatorClientTest {
  private val col = ColumnName("description")

  test("produce create or update mutation scription") {
    val addInst = SecondaryAddIndexInstruction(col, JObject(Map("enabled" -> JBoolean.canonicalTrue)))
    addInst.toString must equal (normalizeWhitespace(s"""{"c":"secondary add index","field_name":"description","directives":{"enabled":true}}"""))
  }


  test("produce drop mutation script") {
    val dropInst = new SecondaryDeleteIndexInstruction(col)
    dropInst.toString must equal (normalizeWhitespace(s"""{"c":"secondary delete index","field_name":"description"}"""))
  }
}
