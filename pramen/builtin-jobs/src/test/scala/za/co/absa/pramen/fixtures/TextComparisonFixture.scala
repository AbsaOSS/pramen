package za.co.absa.pramen.fixtures

import org.scalatest.{Assertion, Suite}

trait TextComparisonFixture {
  this: Suite =>

  protected def compareText(actual: String, expected: String): Assertion = {
    if (actual.replaceAll("[\r\n]", "") != expected.replaceAll("[\r\n]", "")) {
      fail(renderTextDifference(actual, expected))
    } else {
      succeed
    }
  }

  protected def compareTextVertical(actual: String, expected: String): Unit = {
    if (actual.replaceAll("[\r\n]", "") != expected.replaceAll("[\r\n]", "")) {
      fail(s"ACTUAL:\n$actual\nEXPECTED: \n$expected")
    }
  }

  // When comparing INFO files need to ignore 'processStartTime', 'processEndTime' and software version.
  protected def assertInfoFile(actual: String, expected: String): Assertion = {
    val actualInfoFileContents = actual
      .split('\n')
      .filterNot(_.contains("Time"))
      .filterNot(_.contains("\"version\""))
      .mkString("\n")

    compareText(actualInfoFileContents, expected)
  }

  protected def renderTextDifference(textActual: String, textExpected: String): String = {
    val t1 = textActual.replaceAll("\\r\\n", "\\n").split('\n')
    val t2 = textExpected.replaceAll("\\r\\n", "\\n").split('\n')

    val maxLen = Math.max(getMaxStrLen(t1), getMaxStrLen(t2))
    val header = s" ${rightPad("ACTUAL:", maxLen)} ${rightPad("EXPECTED:", maxLen)}\n"

    val stringBuilder = new StringBuilder
    stringBuilder.append(header)

    val linesCount = Math.max(t1.length, t2.length)
    var i = 0
    while (i < linesCount) {
      val a = if (i < t1.length) t1(i) else ""
      val b = if (i < t2.length) t2(i) else ""

      val marker1 = if (a != b) ">" else " "
      val marker2 = if (a != b) "<" else " "

      val comparisonText = s"$marker1${rightPad(a, maxLen)} ${rightPad(b, maxLen)}$marker2\n"
      stringBuilder.append(comparisonText)

      i += 1
    }

    val footer = s"\nACTUAL:\n$textActual"
    stringBuilder.append(footer)
    stringBuilder.toString()
  }

  def getMaxStrLen(text: Seq[String]): Int = {
    if (text.isEmpty) {
      0
    } else {
      text.maxBy(_.length).length
    }
  }

  def rightPad(s: String, length: Int): String = {
    if (s.length < length) {
      s + " " * (length - s.length)
    } else if (s.length > length) {
      s.take(length)
    } else {
      s
    }
  }
}
