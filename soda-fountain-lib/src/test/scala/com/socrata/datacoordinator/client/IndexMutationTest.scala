package com.socrata.datacoordinator.client

import com.socrata.soda.clients.datacoordinator._
import com.socrata.soda.server.id.IndexName

class IndexMutationTest extends DataCoordinatorClientTest {

  val name = new IndexName("col1_col2")
  val expressions = "col1 desc,col2"
  val filter = Some("delete_at is null")

  test("create or update toString produces JSON") {
    val ac = CreateOrUpdateIndexInstruction(name, expressions, filter)
    ac.toString must equal (normalizeWhitespace(s"{c:'create or update index', name:'$name', expressions:'${expressions}', filter:'${filter.get}'}"))
  }

  test("drop toString produces JSON") {
    val ac = DropIndexInstruction(name)
    ac.toString must equal (normalizeWhitespace(s"{c:'drop index', name:'$name'}"))
  }
}
