/*
 * Copyright 2022 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.pramen.bulkload.model

sealed trait BulkLoadPhase {
  def name: String
}

object BulkLoadPhase {
  case object Pending extends BulkLoadPhase {
    override val name: String = "Pending"
  }

  case object Processed extends BulkLoadPhase {
    override val name: String = "Processed"
  }

  case object Repartition1 extends BulkLoadPhase {
    override val name: String = "Repartition1"
  }

  case object Repartition2 extends BulkLoadPhase {
    override val name: String = "Repartition2"
  }

  case object Done extends BulkLoadPhase {
    override val name: String = "Done"
  }

  def fromString(phaseStr: String): BulkLoadPhase = phaseStr match {
    case Pending.name      => Pending
    case Processed.name    => Processed
    case Repartition1.name => Repartition1
    case Repartition2.name => Repartition2
    case Done.name         => Done
    case _                 => throw new IllegalArgumentException(s"Unknown phase: $phaseStr")
  }
}
