package com.socrata.datacoordinator.client

import java.io._
import com.rojoma.json.ast._
import com.rojoma.json.util.JsonUtil

class MutationScript(
        dataset: String,
        user: String,
        copyInstruction: DatasetCopyInstruction,
        instructions: Iterable[Either[ColumnMutationInstruction,RowUpdateInstruction]]){

  def streamJson(writer: Writer){
    var rowDataDeclared = false
    val out = new BufferedWriter(writer)
    out.write('[')
    out.write(JsonUtil.renderJson(JObject(topLevelCommand)))
    instructions.foreach{ instruction =>
      out.write(',')
      instruction match {
        case Left(i)  => {
          if (rowDataDeclared){
            out.write("null,")
            rowDataDeclared = false
          }
          out.write(i.toString)
        }
        case Right(i) => {
          if (!rowDataDeclared) {
            out.write("{\"c\":\"row data\"},")
            rowDataDeclared = true
          }
          out.write(i.toString)
        }
      }
    }
    out.write(']')
    out.flush()
  }

  private val topLevelCommandBase = Map(
    "c"       -> JString(copyInstruction.command),
    "dataset" -> JString(dataset),
    "user"    -> JString(user))

  def topLevelCommand: Map[String, JValue] = {
    copyInstruction match {
      case i: CreateDataset => topLevelCommandBase + ("locale" -> JString(i.locale))
      case i: CopyDataset   => topLevelCommandBase + ("copy_data" -> JBoolean(i.copyData))
      case i: PublishDataset=> topLevelCommandBase + ("snapshot_limit" -> JNumber(i.snapshotLimit))
      case _ => topLevelCommandBase
    }
  }

}
