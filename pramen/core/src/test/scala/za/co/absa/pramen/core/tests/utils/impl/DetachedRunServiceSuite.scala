package za.co.absa.pramen.core.tests.utils.impl

import com.github.yruslan.channel.Channel
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.utils.impl._

import scala.concurrent.Await
import scala.concurrent.duration._

class DetachedRunServiceSuite extends AnyWordSpec {
  "runDetached" should {
    "execution should return the expected result" in {
      val fut = DetachedRunService.runDetached {
        42
      }

      val result = Await.result(fut, 5.seconds)
      assert(result == 42)
    }

    "execution should be delayed" in {
      val channel = Channel.make[Int]
      val fut = DetachedRunService.runDetached {
        channel.recv()
      }

      assert(!fut.isCompleted)
      channel.send(42)

      val result = Await.result(fut, 5.seconds)
      assert(result == 42)
    }

    "execution should propagate the exception" in {
      val fut = DetachedRunService.runDetached {
        throw new RuntimeException("Test")
        42
      }

      val ex = intercept[RuntimeException] {
        Await.result(fut, 5.seconds)
      }
      assert(ex.getMessage == "Test")
    }
  }
}
