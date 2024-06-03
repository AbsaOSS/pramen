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

package za.co.absa.pramen.extras.mocks

import org.apache.http.HttpStatus
import za.co.absa.pramen.extras.utils.httpclient.{SimpleHttpClient, SimpleHttpRequest, SimpleHttpResponse}

import scala.collection.mutable.ListBuffer

class SimpleHttpClientSpy(response: SimpleHttpResponse = SimpleHttpResponse(HttpStatus.SC_OK, Some("body"), Seq.empty),
                          failResponse: Option[SimpleHttpResponse] = None,
                          failNTimes: Int = 0)
  extends SimpleHttpClient {
  var executeCalled = 0
  var closeCalled = 0
  var failuresLeft: Int = failNTimes
  val requests = new ListBuffer[SimpleHttpRequest]

  override def execute(request: SimpleHttpRequest): SimpleHttpResponse = {
    executeCalled += 1
    requests += request

    if (failuresLeft > 0) {
      failuresLeft -= 1
      failResponse match {
        case Some(resp) =>
          resp
        case None =>
          throw new RuntimeException("Test Exception")
      }
    } else {
      response
    }
  }

  override def close(): Unit = closeCalled += 1
}
