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

package za.co.absa.pramen.core.tests.expr.parser

import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.expr.exceptions.SyntaxErrorException
import za.co.absa.pramen.core.expr.lexer.Token._
import za.co.absa.pramen.core.expr.lexer.{Lexer, Token}

class LexerSuite extends AnyWordSpec {
  "Lexer" should {
    "parse a whitespace" in {
      assertTokens(" ", "")
    }
    "parse a whitespaces" in {
      assertTokens(" \t    \t\t", "")
    }

    "parse a variable prefix" in {
      assertTokens("@", "0VR")
    }

    "parse an open parenthesis" in {
      assertTokens("(", "0OP")
    }

    "parse a closing parenthesis" in {
      assertTokens(")", "0CP")
    }

    "parse a plus character" in {
      assertTokens("+", "0PL")
    }

    "parse a minus character" in {
      assertTokens("-", "0MN")
    }

    "parse a name" in {
      assertTokens("name", "0#name#")
    }

    "parse a complex name" in {
      assertTokens("startOf_Date77First", "0#startOf_Date77First#")
    }

    "parse a numeric literal" in {
      assertTokens("123", "0%123%")
    }

    "parse a date literal" in {
      assertTokens("'2020-12-29'", "0@2020-12-29@")
    }

    "multiple tokens" in {
      assertTokens(" 100 + dateOf(@month, -21) - 9",
        "1%100%,5PL,7#dateOf#,13OP,14VR,15#month#,20CM,22MN,23%21%,25CP,27MN,29%9%")
    }

    "throw an exception on an unexpected character" in {
      val ex = intercept[SyntaxErrorException] {
        new Lexer(" . ").lex()
      }
      assert(ex.getMessage == "Unexpected character '.' at position: 1")
    }
  }

  private def assertTokens(expr: String, expected: String): Unit = {
    val lexer = new Lexer(expr)
    val tokens = lexer.lex()

    assert(tokensToString(tokens) == expected)
  }

  private def tokensToString(tokens: List[Token]): String = {
    tokens.map {
      case VAR_PREFIX(pos) => s"${pos}VR"
      case COMMA(pos) => s"${pos}CM"
      case OPEN_PARAN(pos) => s"${pos}OP"
      case CLOSE_PARAN(pos) => s"${pos}CP"
      case PLUS(pos) => s"${pos}PL"
      case MINUS(pos) => s"${pos}MN"
      case NAME(pos, s) => s"$pos#$s#"
      case NUM_LITERAL(pos, s) => s"$pos%$s%"
      case DATE_LITERAL(pos, s) => s"$pos@$s@"
    }.mkString(",")
  }

}
