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

import org.apache.http.client.methods.HttpEntityEnclosingRequestBase

import java.net.URI

/**
  *  This class allows a workaround for sending GET requests with body.
  *
  *  The implementation idea is based on https://stackoverflow.com/a/43265866/1038282
  */
class HttpGetWithBody extends HttpEntityEnclosingRequestBase {
  import HttpGetWithBody._

  def getMethod: String = METHOD_NAME

  def this(uri: String) {
    this()
    setURI(URI.create(uri))
  }
}

object HttpGetWithBody {
  val METHOD_NAME = "GET"
}
