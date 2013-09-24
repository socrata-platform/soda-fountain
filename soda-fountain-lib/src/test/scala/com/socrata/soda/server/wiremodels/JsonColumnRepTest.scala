package com.socrata.soda.server.wiremodels

import org.scalatest.FunSuite
import org.scalatest.matchers.MustMatchers
import com.socrata.soql.types.SoQLType

class JsonColumnRepTest extends FunSuite with MustMatchers {
  test("Client reps know about all types") {
    pendingUntilFixed {
      JsonColumnRep.forClientType.keySet must equal (SoQLType.typesByName.values.toSet)
    }
  }

  test("Data coordinator reps know about all types") {
    pendingUntilFixed {
      JsonColumnRep.forDataCoordinatorType.keySet must equal (SoQLType.typesByName.values.toSet)
    }
  }
}
