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

package za.co.absa.pramen.core.tests.reader

import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.reader.JdbcUrlSelectorImpl
import za.co.absa.pramen.core.reader.model.JdbcConfig

class JdbcUrlSelectorImplSuite extends AnyWordSpec {
  "constructor" should {
    "throw an exception if no URLs defined" in {
      val ex = intercept[RuntimeException] {
        getJdbcUrlSelector(None, Nil)
      }

      assert(ex.getMessage.contains("No JDBC URLs specified"))
    }

    "throw an exception if an empty primary URL is defined" in {
      val ex = intercept[IllegalArgumentException] {
        getJdbcUrlSelector(Some(""), "jdbc://localhost" :: Nil)
      }

      assert(ex.getMessage.contains("Empty string is not a valid JDBC URL"))
    }

    "throw an exception if an empty fallback URL is defined" in {
      val ex = intercept[IllegalArgumentException] {
        getJdbcUrlSelector(Some("jdbc://localhost1"), "jdbc://localhost2" :: "" :: Nil)
      }

      assert(ex.getMessage.contains("Empty string is not a valid JDBC URL"))
    }
  }

  "getUrl()" should {
    "return primary URL if available" in {
      val sel = getJdbcUrlSelector(Some("jdbc://localhost1"), "jdbc://localhost2" :: "jdbc://localhost3" :: Nil)

      assert(sel.getUrl ==  "jdbc://localhost1")
      assert(sel.getUrl ==  "jdbc://localhost1")
      assert(sel.getUrl ==  "jdbc://localhost1")
    }

    "return a fallback URL is primary URL is not available" in {
      val sel = getJdbcUrlSelector(None, "jdbc://localhost2" :: Nil)

      assert(sel.getUrl ==  "jdbc://localhost2")
      assert(sel.getUrl ==  "jdbc://localhost2")
      assert(sel.getUrl ==  "jdbc://localhost2")
    }

    "return one of several fallback urls" in {
      val sel = getJdbcUrlSelector(None, "jdbc://localhost1" :: "jdbc://localhost2" :: "jdbc://localhost2" :: Nil)

      assert(sel.getUrl ==  "jdbc://localhost1" || sel.getUrl ==  "jdbc://localhost2" || sel.getUrl ==  "jdbc://localhost3")
    }
  }

  "getNextUrl()" should {
    "return the same url if only primary is defined" in {
      val sel = getJdbcUrlSelector(Some("jdbc://localhost1"), Nil)

      assert(sel.getNextUrl == "jdbc://localhost1")
    }

    "return the same url if only 1 fallback URL is defined" in {
      val sel = getJdbcUrlSelector(None, "jdbc://localhost1" :: Nil)

      assert(sel.getNextUrl == "jdbc://localhost1")
    }

    "return a different url if several are defined" in {
      val urls = "jdbc://localhost2" :: "jdbc://localhost3" :: "jdbc://localhost4" :: Nil
      val sel = getJdbcUrlSelector(Some("jdbc://localhost1"), urls)

      val u1 = sel.getUrl
      val u2 = sel.getNextUrl

      assert(u1 == "jdbc://localhost1")
      assert(u2 != u1)
      assert(urls.contains(u2))
    }

    "return all different urls from the pool" in {
      val urls = "jdbc://localhost2" :: "jdbc://localhost3" :: "jdbc://localhost4" :: Nil
      val sel = getJdbcUrlSelector(Some("jdbc://localhost1"), urls)

      val u1 = sel.getUrl
      val u2 = sel.getNextUrl
      val u3 = sel.getNextUrl
      val u4 = sel.getNextUrl

      assert(u1 == "jdbc://localhost1")
      assert(u2 != u1)
      assert(u3 != u1)
      assert(u3 != u2)
      assert(u4 != u1)
      assert(u4 != u2)
      assert(u4 != u3)
      assert(urls.contains(u2))
      assert(urls.contains(u3))
      assert(urls.contains(u4))
    }

    "return an url again if the number of retries is bigger than the pool size" in {
      val urls = "jdbc://localhost2" :: "jdbc://localhost3" :: "jdbc://localhost4" :: Nil
      val sel = getJdbcUrlSelector(Some("jdbc://localhost1"), urls)

      val u1 = sel.getUrl
      val u2 = sel.getNextUrl
      val u3 = sel.getNextUrl
      val u4 = sel.getNextUrl
      val u5 = sel.getNextUrl

      assert(u1 == "jdbc://localhost1")
      assert(u2 != u1)
      assert(u3 != u1)
      assert(u3 != u2)
      assert(u4 != u1)
      assert(u4 != u2)
      assert(u4 != u3)
      assert(urls.contains(u2))
      assert(urls.contains(u3))
      assert(urls.contains(u4))

      assert(u5 == u1 || u5 == u2 || u5 == u3 || u5 == u4)
    }
  }

  private def getJdbcUrlSelector(primaryUrl: Option[String], fallbackUrls: Seq[String]): JdbcUrlSelectorImpl = {
    val conf = JdbcConfig("testDriver", primaryUrl, fallbackUrls, None, Some("testUser"), Some("testPassword"))
    new JdbcUrlSelectorImpl(conf)
  }
}
