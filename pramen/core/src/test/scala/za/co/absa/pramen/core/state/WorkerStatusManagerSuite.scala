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

package za.co.absa.pramen.core.state

import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.{CountDownLatch, TimeUnit}

class WorkerStatusManagerSuite extends AnyWordSpec {
  "setStatus" should {
    "register the status of the current thread" in {
      try {
        WorkerStatusManager.setStatus("running task 1")

        val statuses = WorkerStatusManager.getStatuses

        assert(statuses.nonEmpty)
        assert(statuses.exists(s => s.threadId == Thread.currentThread().getId && s.status == "running task 1"))
      } finally {
        WorkerStatusManager.setFinished()
      }
    }

    "overwrite the previous status of the same thread" in {
      try {
        WorkerStatusManager.setStatus("running task 1")
        WorkerStatusManager.setStatus("running task 2")

        val statuses = WorkerStatusManager.getStatuses.filter(_.threadId == Thread.currentThread().getId)

        assert(statuses.length == 1)
        assert(statuses.head.status == "running task 2")
      } finally {
        WorkerStatusManager.setFinished()
      }
    }
  }

  "setFinished" should {
    "remove the status of the current thread" in {
      WorkerStatusManager.setStatus("running task 1")
      WorkerStatusManager.setFinished()

      val statuses = WorkerStatusManager.getStatuses.filter(_.threadId == Thread.currentThread().getId)

      assert(statuses.isEmpty)
    }

    "do nothing if the current thread has no registered status" in {
      WorkerStatusManager.setFinished()
      WorkerStatusManager.setFinished()

      val statuses = WorkerStatusManager.getStatuses.filter(_.threadId == Thread.currentThread().getId)

      assert(statuses.isEmpty)
    }
  }

  "getStatuses" should {
    "return an empty collection when no statuses are registered" in {
      WorkerStatusManager.setFinished()

      assert(WorkerStatusManager.getStatuses.isEmpty)
    }

    "return statuses of all running threads" in {
      val threadsStarted = new CountDownLatch(2)
      val allowToFinish = new CountDownLatch(1)

      val threads = (1 to 2).map { i =>
        val thread = new Thread(new Runnable {
          override def run(): Unit = {
            WorkerStatusManager.setStatus(s"worker $i")
            threadsStarted.countDown()
            allowToFinish.await(10, TimeUnit.SECONDS)
            WorkerStatusManager.setFinished()
          }
        })
        thread.start()
        thread
      }

      threadsStarted.await(10, TimeUnit.SECONDS)

      val statuses = WorkerStatusManager.getStatuses

      allowToFinish.countDown()
      threads.foreach(_.join(10000))

      assert(statuses.length == 2)
      assert(statuses.map(_.status).sortBy(identity) == Seq("worker 1", "worker 2"))
      assert(statuses.map(_.threadId).distinct.length == 2)
      assert(WorkerStatusManager.getStatuses.isEmpty)
    }
  }
}
