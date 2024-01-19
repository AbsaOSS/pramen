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

package za.co.absa.pramen.core.metastore.persistence

import org.apache.spark.sql.DataFrame
import org.mockito.Mockito.mock
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.TempDirFixture
import za.co.absa.pramen.core.metastore.peristence.TransientJobManager
import za.co.absa.pramen.core.runner.task.TaskRunner

import java.time.LocalDate

class TransientJobManagerSuite  extends AnyWordSpec with BeforeAndAfterAll with SparkTestBase with TempDirFixture {
  import spark.implicits._

  private val infoDate = LocalDate.of(2022, 2, 18)

  private def exampleDf: DataFrame = List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")

  private var tempDir: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    tempDir = createTempDir("transient_persist")
  }

  override def afterAll(): Unit = {
    deleteDir(tempDir)

    super.afterAll()
  }

  "setTaskRunner" should {
    "set the specified task runner" in {
      val taskRunner = mock(classOf[TaskRunner])

      assert(!TransientJobManager.hasTaskRunner)

      TransientJobManager.setTaskRunner(taskRunner)

      assert(TransientJobManager.hasTaskRunner)

      TransientJobManager.reset()
    }
  }
}
