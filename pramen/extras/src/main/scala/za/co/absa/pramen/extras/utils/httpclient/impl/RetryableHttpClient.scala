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

package za.co.absa.pramen.extras.utils.httpclient.impl

import org.slf4j.LoggerFactory
import za.co.absa.pramen.extras.utils.httpclient.{SimpleHttpClient, SimpleHttpRequest, SimpleHttpResponse}

import scala.util.control.NonFatal

class RetryableHttpClient(httpClient: SimpleHttpClient, numberOfRetries: Int, backoffMs: Int) extends SimpleHttpClient {
  private val log = LoggerFactory.getLogger(this.getClass)

  override def execute(request: SimpleHttpRequest): SimpleHttpResponse = {
    var retries = 0
    var response: SimpleHttpResponse = null
    while (response == null && retries < numberOfRetries)
      try
        response = httpClient.execute(request)
      catch {
        case NonFatal(ex) =>
          retries += 1
          if (retries >= numberOfRetries) {
            throw ex
          }
          Thread.sleep(backoffMs)
          log.error(
            s"${request.method.name} Request to ${request.url} failed. Retrying... (attempt $retries/$numberOfRetries)",
            ex
          )
      }
    response
  }

  override def close(): Unit = httpClient.close()
}

object RetryableHttpClient {
  val DEFAULT_NUMBER_OF_RETRIES = 3
  val DEFAULT_MAXIMUM_BACKOFF_MS = 3000
}
