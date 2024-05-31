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

import org.slf4j.LoggerFactory
import za.co.absa.pramen.extras.utils.httpclient.impl.{BasicHttpClient, RetryableHttpClient}

trait SimpleHttpClient extends AutoCloseable {
  def execute(request: SimpleHttpRequest): SimpleHttpResponse
}

object SimpleHttpClient {
  private val log = LoggerFactory.getLogger(this.getClass)

  /**
    * This builds an instance of `SimpleHttpClient` that should be used in practice.
    *
    * You can customize the retry policy.
    *
    * @param trustAllSslCerts If true, the client will trust all SSL certificates.
    * @param numberOfRetries  The number of retries to be done in case of failure.
    * @param backoffMs        The maximum backoff time in milliseconds between retries.
    * @return An instance of `SimpleHttpClient`.
    */
  def apply(trustAllSslCerts: Boolean = false,
            numberOfRetries: Int = RetryableHttpClient.DEFAULT_NUMBER_OF_RETRIES,
            backoffMs: Int = RetryableHttpClient.DEFAULT_MAXIMUM_BACKOFF_MS): SimpleHttpClient = {
    log.info("Creating a default HTTP client with the following settings:")
    log.info(s"  Trust all CA certificates: $trustAllSslCerts")
    log.info(s"  Number of retries: $numberOfRetries")
    log.info(s"  Maximum backoff between retries: $backoffMs ms")

    new RetryableHttpClient(new BasicHttpClient(trustAllSslCerts), numberOfRetries, backoffMs)
  }
}
