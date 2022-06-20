package za.co.absa.pramen.framework.tests.utils

import org.scalatest.WordSpec
import za.co.absa.pramen.framework.utils.DateUtils._

import java.time.{LocalDate, ZoneId}

class DateUtilsSuite extends WordSpec {

  "convertStrToDate()" should {
    "Use the first pattern if it is parsable" in {
      assert(convertStrToDate("2020-10-30", "yyyy-MM-dd", "dummy") == LocalDate.of(2020, 10, 30))
    }

    "Use the second pattern if it is parsable" in {
      assert(convertStrToDate("20201030", "yyyy-MM-dd", "yyyyMMdd") == LocalDate.of(2020, 10, 30))
    }

    "Fail if both patterns are not suitable" in {
      val ex = intercept[IllegalArgumentException] {
        convertStrToDate("20201030", "yyyy-MM-dd", "yyyy/MMd/dd")
      }

      assert(ex.getMessage.contains(s"Cannot parse '20201030' in one of 'yyyy-MM-dd', 'yyyy/MMd/dd'"))
    }

    "Fail if no patterns are specified" in {
      val ex = intercept[IllegalArgumentException] {
        convertStrToDate("20201030")
      }

      assert(ex.getMessage.contains(s"No patterns provided to parse date '20201030'"))
    }
  }

  "fromDateToTimestampMs" should {
    "return the timestamp in milliseconds" in {
      val date = LocalDate.of(2020, 10, 30)
      assert(fromDateToTimestampMs(date, ZoneId.of("UTC")) == 1604016000000L)
    }
  }

}
