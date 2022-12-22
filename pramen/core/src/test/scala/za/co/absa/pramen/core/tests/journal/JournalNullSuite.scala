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

package za.co.absa.pramen.core.tests.journal

import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.journal.{Journal, JournalNull}

class JournalNullSuite extends AnyWordSpec {
  import TestCases._

  "addEntry()" should {
    "do nothing" in {
      val journal = getJournal

      val entries = journal.getEntries(instant2, instant3)

      assert(entries.isEmpty)
    }
  }

  "getEntries()" should {
    "do nothing" in {
      val journal = getJournal

      val entries = journal.getEntries(instant1, instant3)

      assert(entries.isEmpty)
    }
  }

  def getJournal: Journal = {
    val journal = new JournalNull

    journal.addEntry(task1)
    journal.addEntry(task2)
    journal.addEntry(task3)

    journal
  }
}
