package com.socrata.soda.server.util

import com.socrata.soda.clients.datacoordinator.RollupDatasetRelation
import com.socrata.soda.server.id.{CopyId, ResourceName, RollupMapId, RollupName}
import com.socrata.soda.server.model.RollupInfo

import java.sql.ResultSet
import java.time.OffsetDateTime

object ResultSetMapper {

  def extractSetRollupDatasetRelation(rs: ResultSet): Set[RollupDatasetRelation] = {
    Iterator.continually(rs)
      .takeWhile(_.next())
      .map(rs =>
        RollupDatasetRelation(new ResourceName(rs.getString("primary_dataset")), new RollupName(rs.getString("name")), rs.getString("soql"), rs.getArray("secondary_datasets").getArray.asInstanceOf[Array[String]].map(new ResourceName(_)).toSet)
      ).toList.toSet
  }

  def extractSetRollupMapId(rs:ResultSet): Set[RollupMapId] ={
    Iterator.continually(rs)
      .takeWhile(_.next())
      .map(rs =>
        new RollupMapId(rs.getLong("id"))
      ).toList.toSet
  }

  def extractRollupMapId(rs:ResultSet): RollupMapId={
    if (rs.next()) {
      new RollupMapId(rs.getLong(1))
    } else {
      throw new IllegalStateException("There should always be a primary key returned when updating/inserting.")
    }
  }

  def extractRollupMapIdCopyId(rs: ResultSet): (RollupMapId,CopyId) = {
    if (rs.next()) {
      (new RollupMapId(rs.getLong(1)),new CopyId(rs.getLong(2)))
    } else {
      throw new IllegalStateException("There should always be a primary key returned when updating/inserting.")
    }
  }

  def extractSetRollupInfo(rs: ResultSet): Set[RollupInfo] = {
    Iterator.continually(rs)
      .takeWhile(_.next())
      .map(rs =>
        RollupInfo(
          new RollupMapId(rs.getLong("id")),
          new CopyId(rs.getLong("dataset_copy_id")),
          new RollupName(rs.getString("name")),
          rs.getString("soql"),
          rs.getObject("last_accessed",classOf[OffsetDateTime])
        )
      ).toList.toSet
  }


}
