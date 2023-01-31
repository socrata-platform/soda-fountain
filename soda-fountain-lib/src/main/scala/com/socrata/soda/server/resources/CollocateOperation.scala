package com.socrata.soda.server.resources

import com.rojoma.json.v3.codec.DecodeError
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, JsonUtil}
import com.socrata.http.server.HttpRequest
import com.socrata.soda.clients.datacoordinator.DataCoordinatorClient.Cost
import com.socrata.soda.server.highlevel.DatasetDAO
import com.socrata.soda.server.id.{DatasetInternalName, ResourceName}


case class DCCollocateOperation(collocations: Seq[Seq[DatasetInternalName]], limits: Cost)
case class SFCollocateOperation(collocations: Seq[Seq[ResourceName]], limits: Cost)

object SFCollocateOperation{
  private implicit val coCodec = AutomaticJsonCodecBuilder[SFCollocateOperation]
  def getFromRequest(req: HttpRequest): Either[DecodeError, SFCollocateOperation] = {
    JsonUtil.readJson[SFCollocateOperation](req.servletRequest.getReader)
  }
}
object DCCollocateOperation{
  def apply(sfCollocate: SFCollocateOperation, transformer: ResourceName => Option[DatasetInternalName]): Either[DCCollocateOperation, DatasetDAO.FailResult] = {
    val translatedIds = sfCollocate.collocations.map{_.map{
      resource =>
        transformer(resource) match {
          case Some(id) => Left(id)
          case None => Right(DatasetDAO.DatasetNotFound(resource))
        }
    }}

    if(translatedIds.forall(_.forall(_.isLeft))) {
      // Everything was able to be translated
      Left(DCCollocateOperation(translatedIds.map(_.map(_.left.get)), sfCollocate.limits))
    } else {
      // Just get the first error to propogate up
      Right(translatedIds.flatten.find(_.isRight).get.right.get)
    }
  }
}
