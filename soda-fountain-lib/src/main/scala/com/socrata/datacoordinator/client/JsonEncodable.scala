package com.socrata.datacoordinator.client

import com.rojoma.json.ast.JValue

trait JsonEncodable {
  def asJson : JValue
}
