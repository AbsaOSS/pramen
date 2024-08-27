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

package za.co.absa.pramen.core.tests.lock

import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.TempDirFixture
import za.co.absa.pramen.core.lock.TokenLockHadoopPath

class TokenLockHadoopPathSuite extends AnyWordSpec with SparkTestBase with TempDirFixture {
  private val hdfsConfig = spark.sparkContext.hadoopConfiguration

  "Token lock" should {
    "be able to acquire and release locks" in {
      withTempDirectory("simpleLock") { tempDir =>
        val lock1 = new TokenLockHadoopPath("token1", hdfsConfig, tempDir)

        assert(lock1.tryAcquire())
        assert(!lock1.tryAcquire())

        val lock2 = new TokenLockHadoopPath("token1", hdfsConfig, tempDir)
        assert(!lock2.tryAcquire())

        lock1.release()

        assert(lock2.tryAcquire())
        assert(!lock2.tryAcquire())

        lock2.release()
      }
    }

    "multiple token locks should not affect each other" in {
      withTempDirectory("simpleLock") { tempDir =>
        val lock1 = new TokenLockHadoopPath("token1", hdfsConfig, tempDir)
        val lock2 = new TokenLockHadoopPath("token2", hdfsConfig, tempDir)

        assert(lock1.tryAcquire())
        assert(lock2.tryAcquire())

        assert(!lock1.tryAcquire())
        assert(!lock2.tryAcquire())

        lock1.release()

        assert(lock1.tryAcquire())
        assert(!lock2.tryAcquire())

        lock1.release()
        lock2.release()
      }
    }

    "lock pramen should constantly update lock ticket" ignore {
      withTempDirectory("simpleLock") { tempDir =>
        val lock1 = new TokenLockHadoopPath("token1", hdfsConfig, tempDir, 3L)
        val lock2 = new TokenLockHadoopPath("token1", hdfsConfig, tempDir)
        assert(lock1.tryAcquire())
        Thread.sleep(4000)
        assert(!lock2.tryAcquire())
        assert(!lock1.tryAcquire())
        lock1.release()
      }
    }
  }

}
