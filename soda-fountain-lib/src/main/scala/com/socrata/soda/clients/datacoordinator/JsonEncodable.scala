package com.socrata.soda.clients.datacoordinator

import com.rojoma.json.v3.ast.JValue

trait JsonEncodable {
  def asJson : JValue
}
