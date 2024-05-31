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

package za.co.absa.pramen.extras.utils.httpclient

import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.extras.utils.httpclient.impl.{HttpDeleteWithBody, HttpGetWithBody}

class HttpClientSpecialMethodsSuite extends AnyWordSpec {
  "HttpDeleteWithBody" should {
    "return method as DELETE" in {
      val httpDeleteWithBody = new HttpDeleteWithBody()
      assert(httpDeleteWithBody.getMethod == HttpDeleteWithBody.METHOD_NAME)
    }
  }

  "HttpGetWithBody" should {
    "return method as DELETE" in {
      val httpGetWithBody = new HttpGetWithBody()
      assert(httpGetWithBody.getMethod == HttpGetWithBody.METHOD_NAME)
    }
  }
}
