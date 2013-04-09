package com.socrata.datacoordinator

import com.socrata.datacoordinator.client.{MutationContext, JsonEncodable, DatasetCopyInstruction, ColumnMutationInstruction}

package object client {
  type CM = ColumnMutationInstruction
  type DC = DatasetCopyInstruction
  type JE = JsonEncodable
  type MC = MutationContext
}