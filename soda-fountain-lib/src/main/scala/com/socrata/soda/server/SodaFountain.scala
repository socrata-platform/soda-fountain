package com.socrata.soda.server

import com.socrata.soda.server.services._

object SodaFountain {

}

abstract class SodaFountain
  extends SodaService
  with DatasetService
  with RowService
  with ColumnService {


}