package com.socrata.soda.clients.datacoordinator

import com.rojoma.json.ast.JValue

trait JsonEncodable {
  def asJson : JValue
}
