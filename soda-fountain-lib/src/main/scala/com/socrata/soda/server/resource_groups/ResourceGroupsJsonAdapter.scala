package com.socrata.soda.server.resource_groups

import com.rojoma.json.v3.util.JsonUtil

import com.socrata.resource_groups.client.{ResourceGroupsClientBuilder,ResourceGroupsException}
import com.socrata.resource_groups.models.api.ResourceGroupsObject


class ResourceGroupsJsonAdapter extends ResourceGroupsClientBuilder.JsonCodecAdapter {
  @throws[ResourceGroupsException]
  override def readValue[T <: ResourceGroupsObject](content: String, targetClass: Class[T]): T = {
    JsonUtil.parseJson(content) match {
      case Left(err) => throw new ResourceGroupsException(err)
      case Right(value) if targetClass.isAssignableFrom(value.getClass) => value.asInstanceOf[T]
      case Right(_) => throw new IllegalStateException("valid ResourceGroupsObject, but not a valid T")
    }
  }

  @throws[ResourceGroupsException]
  override def writeValueAsString[T <: ResourceGroupsObject](value: T): String = {
    JsonUtil.renderJson(value.asInstanceOf[ResourceGroupsObject], false)
  }
}
