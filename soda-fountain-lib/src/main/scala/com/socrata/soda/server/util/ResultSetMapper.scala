package com.socrata.soda.server.util

import com.socrata.soda.clients.datacoordinator.RollupDatasetRelation
import com.socrata.soda.server.id.{ResourceName, RollupName}

import java.sql.ResultSet

object ResultSetMapper {

  def extractSeqRollupDatasetRelation(rs: ResultSet): Set[RollupDatasetRelation] = {
    Iterator.continually(rs)
      .takeWhile(_.next())
      .map(rs =>
        RollupDatasetRelation(new ResourceName(rs.getString("primary_dataset")), new RollupName(rs.getString("name")), rs.getString("soql"), rs.getArray("secondary_datasets").getArray.asInstanceOf[Array[String]].map(new ResourceName(_)).toSet)
      ).toList.toSet
  }

}
