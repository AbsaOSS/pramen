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

package za.co.absa.pramen.core.tests.runner.task

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.runner.task.ThreadClosableRegistry

import scala.collection.mutable

class ThreadClosableRegistrySuite extends AnyWordSpec with Matchers  {

  private def createCountingCloseable(): (AutoCloseable, () => Int) = {
    var count = 0
    val closeable = new AutoCloseable {
      override def close(): Unit = count += 1
    }
    (closeable, () => count)
  }

  private def currentThreadId: Long = Thread.currentThread().getId

  "ThreadClosableRegistry" should {
    "register and cleanup single or multiple closeable resources" in {
      val (closeable1, getCount1) = createCountingCloseable()
      val (closeable2, getCount2) = createCountingCloseable()
      val (closeable3, getCount3) = createCountingCloseable()

      ThreadClosableRegistry.registerCloseable(closeable1)
      ThreadClosableRegistry.registerCloseable(closeable2)
      ThreadClosableRegistry.registerCloseable(closeable3)

      ThreadClosableRegistry.cleanupThread(currentThreadId)

      getCount1() shouldBe 1
      getCount2() shouldBe 1
      getCount3() shouldBe 1
    }

    "not affect other threads when cleaning up a specific thread" in {
      val (closeableThread1, getCount1) = createCountingCloseable()
      val (closeableThread2, getCount2) = createCountingCloseable()

      var thread1Id: Long = 0
      var thread2Id: Long = 0

      val thread1 = new Thread {
        override def run(): Unit = {
          thread1Id = Thread.currentThread().getId
          ThreadClosableRegistry.registerCloseable(closeableThread1)
        }
      }

      val thread2 = new Thread {
        override def run(): Unit = {
          thread2Id = Thread.currentThread().getId
          ThreadClosableRegistry.registerCloseable(closeableThread2)
        }
      }

      thread1.start()
      thread2.start()
      thread1.join()
      thread2.join()

      // Cleanup only thread1
      ThreadClosableRegistry.cleanupThread(thread1Id)
      getCount1() shouldBe 1
      getCount2() shouldBe 0

      // Cleanup thread2
      ThreadClosableRegistry.cleanupThread(thread2Id)
      getCount2() shouldBe 1
    }

    "handle exceptions during resource cleanup gracefully" in {
      val (closeable1, getCount1) = createCountingCloseable()
      val (closeable3, getCount3) = createCountingCloseable()

      var closeCalled2 = 0
      val closeable2 = new AutoCloseable {
        override def close(): Unit = {
          closeCalled2 += 1
          throw new RuntimeException("Close failed")
        }
      }

      ThreadClosableRegistry.registerCloseable(closeable1)
      ThreadClosableRegistry.registerCloseable(closeable2)
      ThreadClosableRegistry.registerCloseable(closeable3)

      // Should not throw exception, should attempt to close all resources
      noException should be thrownBy ThreadClosableRegistry.cleanupThread(currentThreadId)

      getCount1() shouldBe 1
      closeCalled2 shouldBe 1
      getCount3() shouldBe 1
    }

    "do nothing when cleaning up a thread with no registered resources" in {
      val nonExistentThreadId = 999999L

      noException should be thrownBy ThreadClosableRegistry.cleanupThread(nonExistentThreadId)
    }

    "do nothing when cleaning up an already cleaned thread" in {
      val (closeable, getCount) = createCountingCloseable()

      ThreadClosableRegistry.registerCloseable(closeable)
      ThreadClosableRegistry.cleanupThread(currentThreadId)
      getCount() shouldBe 1

      // Cleanup again - should not call close again
      ThreadClosableRegistry.cleanupThread(currentThreadId)
      getCount() shouldBe 1
    }

    "close resources in LIFO order (last registered, first closed)" in {
      val closeOrder = mutable.ArrayBuffer[Int]()

      val closeables = (1 to 3).map { id =>
        new AutoCloseable {
          override def close(): Unit = closeOrder += id
        }
      }

      closeables.foreach(ThreadClosableRegistry.registerCloseable)
      ThreadClosableRegistry.cleanupThread(currentThreadId)

      closeOrder should contain theSameElementsInOrderAs Seq(3, 2, 1)
    }

    "handle concurrent registrations from multiple threads" in {
      val numThreads = 10
      val closeablesPerThread = 5
      val threadData = mutable.Map[Long, Seq[() => Int]]()

      val threads = (1 to numThreads).map { _ =>
        new Thread {
          override def run(): Unit = {
            val threadId = Thread.currentThread().getId
            val closeablesWithCounters = (1 to closeablesPerThread).map(_ => createCountingCloseable())

            threadData.synchronized {
              threadData(threadId) = closeablesWithCounters.map(_._2)
            }

            closeablesWithCounters.foreach { case (closeable, _) =>
              ThreadClosableRegistry.registerCloseable(closeable)
            }
          }
        }
      }

      threads.foreach(_.start())
      threads.foreach(_.join())

      threadData.foreach { case (threadId, getCounters) =>
        ThreadClosableRegistry.cleanupThread(threadId)
        getCounters.foreach(getCount => getCount() shouldBe 1)
      }
    }

    "not close an explicitly unregistered resource during cleanup" in {
      val (closeable, getCount) = createCountingCloseable()

      ThreadClosableRegistry.registerCloseable(closeable)
      ThreadClosableRegistry.unregisterCloseable(closeable)

      // cleanupThread should not close the unregistered resource
      ThreadClosableRegistry.cleanupThread(currentThreadId)
      getCount() shouldBe 0
    }
  }
}
