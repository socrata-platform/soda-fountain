package com.socrata.datacoordinator.client

import java.io._
import com.rojoma.json.ast._
import com.rojoma.json.util.JsonUtil

class MutationScript(
        user: String,
        copyInstruction: DatasetCopyInstruction,
        instructions: Iterable[dataCoordinatorInstruction]){
  def streamJson(os:OutputStream){
    val out = new OutputStreamWriter(os)
    streamJson(out)
  }
  def streamJson(out: Writer){
    var rowDataDeclared = false
    var declaredOptions = RowUpdateOptionChange()

    out.write('[')
    out.write(JsonUtil.renderJson(JObject(topLevelCommand)))

    val itr = instructions.iterator
    while (itr.hasNext){
      val instruction = itr.next
      out.write(',')
      instruction match {
        case i: ColumnMutationInstruction  => {
          if (rowDataDeclared){
            out.write("null,")
            rowDataDeclared = false
          }
          out.write(i.toString)
        }
        case i: RowUpdateInstruction => {
          i match {
            case ops: RowUpdateOptionChange => {
              if (ops != declaredOptions){
                if (rowDataDeclared){
                  out.write("null,")
                  rowDataDeclared = false
                }
              }
              declaredOptions = ops
              if (!rowDataDeclared){
                out.write(declaredOptions.toString)
                rowDataDeclared = true
              }
            }
            case _ => {
              if (!rowDataDeclared) {
                out.write(declaredOptions.toString)
                out.write(",")
                rowDataDeclared = true
              }
              out.write(i.toString)
            }
          }
        }
      }
    }
    out.write(']')
    out.flush()
  }

  private val topLevelCommandBase = Map(
    "c"       -> JString(copyInstruction.command),
    "user"    -> JString(user))

  def topLevelCommand: Map[String, JValue] = {
    copyInstruction match {
      case i: CreateDataset => topLevelCommandBase + ("locale" -> JString(i.locale))
      case i: CopyDataset   => topLevelCommandBase + ("copy_data" -> JBoolean(i.copyData))
      case i: PublishDataset=> i.snapshotLimit match {
        case Some(s)  => topLevelCommandBase + ("snapshot_limit" -> JNumber(s))
        case None     => topLevelCommandBase
      }
      case _ => topLevelCommandBase
    }
  }

}
