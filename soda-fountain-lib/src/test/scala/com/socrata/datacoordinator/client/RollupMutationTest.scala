package com.socrata.datacoordinator.client

import com.socrata.soda.clients.datacoordinator._
import com.socrata.soda.server.id.RollupName

class RollupMutationTest extends DataCoordinatorClientTest {

  val name = new RollupName("clown_type")
  val soql = "SELECT clown_type, count(*)"

  test("create or update toString produces JSON") {
    val ac = CreateOrUpdateRollupInstruction(name, soql, soql)
    ac.toString must equal (normalizeWhitespace(s"{c:'create or update rollup', name:'$name', soql:'$soql', raw_soql:'$soql'}"))
  }

  test("drop toString produces JSON") {
    val ac = DropRollupInstruction(name)
    ac.toString must equal (normalizeWhitespace(s"{c:'drop rollup', name:'$name'}"))
  }
}
