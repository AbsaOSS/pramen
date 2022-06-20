/*
 * Copyright 2022 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.pramen.framework.tests.bookkeeper

import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfter
import za.co.absa.pramen.framework.base.SparkTestBase
import za.co.absa.pramen.framework.bookkeeper.{SyncBookKeeper, SyncBookKeeperText}
import za.co.absa.pramen.framework.fixtures.TempDirFixture
import za.co.absa.pramen.framework.utils.FsUtils

class SyncBookKeeperTextLongSuite extends SyncBookKeeperCommonSuite with SparkTestBase with BeforeAndAfter with TempDirFixture {

  import za.co.absa.pramen.framework.bookkeeper.SyncBookKeeperText._

  private val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, "/tmp")

  var tmpDir: String = _

  before {
    tmpDir = createTempDir("bkHadoopSuite")
  }

  after {
    deleteDir(tmpDir)
  }

  def getBookkeeper: SyncBookKeeper = {
    new SyncBookKeeperText(tmpDir)
  }

  "BookkeeperHadoopText" when {
    "initialized" should {
      "Initialize bookkeeping directory" in {
        getBookkeeper

        assert(fsUtils.exists(new Path(tmpDir, s"$bookkeepingRootPath/$recordsDirName")))
        assert(fsUtils.exists(new Path(tmpDir, locksDirName)))
      }
    }

    testBookKeeper(() => getBookkeeper)
  }

}
