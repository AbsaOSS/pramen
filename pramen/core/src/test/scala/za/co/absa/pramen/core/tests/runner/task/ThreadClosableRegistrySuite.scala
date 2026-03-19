
package za.co.absa.pramen.core.tests.runner.task

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.runner.task.ThreadClosableRegistry

import scala.collection.mutable

class ThreadClosableRegistrySuite extends AnyWordSpec with Matchers  {

  "ThreadClosableRegistry" should {
    "register a closeable resource for the current thread" in {
      var closeCalled = 0
      val closeable = new AutoCloseable {
        override def close(): Unit = closeCalled += 1
      }
      val threadId = Thread.currentThread().getId

      ThreadClosableRegistry.registerCloseable(closeable)

      // Cleanup to verify it was registered
      ThreadClosableRegistry.cleanupThread(threadId)
      closeCalled shouldBe 1
    }

    "register multiple closeable resources for the same thread" in {
      var closeCalled1 = 0
      var closeCalled2 = 0
      var closeCalled3 = 0
      val closeable1 = new AutoCloseable {
        override def close(): Unit = closeCalled1 += 1
      }
      val closeable2 = new AutoCloseable {
        override def close(): Unit = closeCalled2 += 1
      }
      val closeable3 = new AutoCloseable {
        override def close(): Unit = closeCalled3 += 1
      }
      val threadId = Thread.currentThread().getId

      ThreadClosableRegistry.registerCloseable(closeable1)
      ThreadClosableRegistry.registerCloseable(closeable2)
      ThreadClosableRegistry.registerCloseable(closeable3)

      ThreadClosableRegistry.cleanupThread(threadId)

      closeCalled1 shouldBe 1
      closeCalled2 shouldBe 1
      closeCalled3 shouldBe 1
    }

    "cleanup all registered resources for a specific thread" in {
      var closeCalled1 = 0
      var closeCalled2 = 0
      val closeable1 = new AutoCloseable {
        override def close(): Unit = closeCalled1 += 1
      }
      val closeable2 = new AutoCloseable {
        override def close(): Unit = closeCalled2 += 1
      }

      val threadId = Thread.currentThread().getId

      ThreadClosableRegistry.registerCloseable(closeable1)
      ThreadClosableRegistry.registerCloseable(closeable2)

      ThreadClosableRegistry.cleanupThread(threadId)

      closeCalled1 shouldBe 1
      closeCalled2 shouldBe 1
    }

    "not affect other threads when cleaning up a specific thread" in {
      var closeCalled1 = 0
      var closeCalled2 = 0
      val closeableThread1 = new AutoCloseable {
        override def close(): Unit = closeCalled1 += 1
      }
      val closeableThread2 = new AutoCloseable {
        override def close(): Unit = closeCalled2 += 1
      }

      val thread1Results = mutable.ArrayBuffer[Long]()
      val thread2Results = mutable.ArrayBuffer[Long]()

      val thread1 = new Thread(() => {
        thread1Results += Thread.currentThread().getId
        ThreadClosableRegistry.registerCloseable(closeableThread1)
      })

      val thread2 = new Thread(() => {
        thread2Results += Thread.currentThread().getId
        ThreadClosableRegistry.registerCloseable(closeableThread2)
      })

      thread1.start()
      thread2.start()
      thread1.join()
      thread2.join()

      val thread1Id = thread1Results.head
      val thread2Id = thread2Results.head

      // Cleanup only thread1
      ThreadClosableRegistry.cleanupThread(thread1Id)

      closeCalled1 shouldBe 1
      closeCalled2 shouldBe 0

      // Cleanup thread2
      ThreadClosableRegistry.cleanupThread(thread2Id)
      closeCalled2 shouldBe 1
    }

    "handle exceptions during resource cleanup gracefully" in {
      var closeCalled1 = 0
      var closeCalled2 = 0
      var closeCalled3 = 0
      val closeable1 = new AutoCloseable {
        override def close(): Unit = closeCalled1 += 1
      }
      val closeable2 = new AutoCloseable {
        override def close(): Unit = {
          closeCalled2 += 1
          throw new RuntimeException("Close failed")
        }
      }
      val closeable3 = new AutoCloseable {
        override def close(): Unit = closeCalled3 += 1
      }

      val threadId = Thread.currentThread().getId

      ThreadClosableRegistry.registerCloseable(closeable1)
      ThreadClosableRegistry.registerCloseable(closeable2)
      ThreadClosableRegistry.registerCloseable(closeable3)

      // Should not throw exception, should attempt to close all resources
      noException should be thrownBy ThreadClosableRegistry.cleanupThread(threadId)

      closeCalled1 shouldBe 1
      closeCalled2 shouldBe 1
      closeCalled3 shouldBe 1
    }

    "do nothing when cleaning up a thread with no registered resources" in {
      val nonExistentThreadId = 999999L

      noException should be thrownBy ThreadClosableRegistry.cleanupThread(nonExistentThreadId)
    }

    "do nothing when cleaning up an already cleaned thread" in {
      var closeCalled = 0
      val closeable = new AutoCloseable {
        override def close(): Unit = closeCalled += 1
      }
      val threadId = Thread.currentThread().getId

      ThreadClosableRegistry.registerCloseable(closeable)
      ThreadClosableRegistry.cleanupThread(threadId)

      closeCalled shouldBe 1

      // Cleanup again - should not call close again
      ThreadClosableRegistry.cleanupThread(threadId)

      closeCalled shouldBe 1
    }

    "close resources in LIFO order (last registered, first closed)" in {
      val closeOrder = mutable.ArrayBuffer[Int]()

      val closeable1 = new AutoCloseable {
        override def close(): Unit = closeOrder += 1
      }
      val closeable2 = new AutoCloseable {
        override def close(): Unit = closeOrder += 2
      }
      val closeable3 = new AutoCloseable {
        override def close(): Unit = closeOrder += 3
      }

      val threadId = Thread.currentThread().getId

      ThreadClosableRegistry.registerCloseable(closeable1)
      ThreadClosableRegistry.registerCloseable(closeable2)
      ThreadClosableRegistry.registerCloseable(closeable3)

      ThreadClosableRegistry.cleanupThread(threadId)

      closeOrder should contain theSameElementsInOrderAs Seq(3, 2, 1)
    }

    "handle concurrent registrations from multiple threads" in {
      val numThreads = 10
      val closeablesPerThread = 5
      val allCloseables = mutable.Map[Long, Seq[(AutoCloseable, () => Int)]]()

      val threads = (1 to numThreads).map { i =>
        new Thread(() => {
          val threadId = Thread.currentThread().getId
          val closeables = (1 to closeablesPerThread).map { _ =>
            var closeCalled = 0
            val closeable = new AutoCloseable {
              override def close(): Unit = closeCalled += 1
            }
            (closeable, () => closeCalled)
          }
          allCloseables.synchronized {
            allCloseables(threadId) = closeables
          }
          closeables.foreach { case (closeable, _) => ThreadClosableRegistry.registerCloseable(closeable) }
        })
      }

      threads.foreach(_.start())
      threads.foreach(_.join())

      allCloseables.foreach { case (threadId, closeables) =>
        ThreadClosableRegistry.cleanupThread(threadId)
        closeables.foreach { case (_, getCloseCalled) =>
          getCloseCalled() shouldBe 1
        }
      }
    }
  }
}
