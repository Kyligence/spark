/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.util

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.util.StringUtils._

class StringUtilsSuite extends SparkFunSuite {

  test("escapeLikeRegex") {
    val expectedEscapedStrOne = "(?s)\\Qa\\E\\Qb\\E\\Qd\\E\\Qe\\E\\Qf\\E"
    val expectedEscapedStrTwo = "(?s)\\Qa\\E\\Q_\\E.\\Qb\\E"
    val expectedEscapedStrThree = "(?s)\\Qa\\E..*\\Qb\\E"
    val expectedEscapedStrFour = "(?s)\\Qa\\E.*\\Q%\\E\\Qb\\E"
    val expectedEscapedStrFive = "(?s)\\Qa\\E.*"
    val expectedEscapedStrSix = "(?s)\\Q*\\E\\Q*\\E"
    val expectedEscapedStrSeven = "(?s)\\Qa\\E.\\Qb\\E"
    assert(escapeLikeRegex("abdef", '\\') === expectedEscapedStrOne)
    assert(escapeLikeRegex("abdef", '/') === expectedEscapedStrOne)
    assert(escapeLikeRegex("abdef", '\"') === expectedEscapedStrOne)
    assert(escapeLikeRegex("a\\__b", '\\') === expectedEscapedStrTwo)
    assert(escapeLikeRegex("a/__b", '/') === expectedEscapedStrTwo)
    assert(escapeLikeRegex("a\"__b", '\"') === expectedEscapedStrTwo)
    assert(escapeLikeRegex("a_%b", '\\') === expectedEscapedStrThree)
    assert(escapeLikeRegex("a_%b", '/') === expectedEscapedStrThree)
    assert(escapeLikeRegex("a_%b", '\"') === expectedEscapedStrThree)
    assert(escapeLikeRegex("a%\\%b", '\\') === expectedEscapedStrFour)
    assert(escapeLikeRegex("a%/%b", '/') === expectedEscapedStrFour)
    assert(escapeLikeRegex("a%\"%b", '\"') === expectedEscapedStrFour)
    assert(escapeLikeRegex("a%", '\\') === expectedEscapedStrFive)
    assert(escapeLikeRegex("a%", '/') === expectedEscapedStrFive)
    assert(escapeLikeRegex("a%", '\"') === expectedEscapedStrFive)
    assert(escapeLikeRegex("**", '\\') === expectedEscapedStrSix)
    assert(escapeLikeRegex("**", '/') === expectedEscapedStrSix)
    assert(escapeLikeRegex("**", '\"') === expectedEscapedStrSix)
    assert(escapeLikeRegex("a_b", '\\') === expectedEscapedStrSeven)
    assert(escapeLikeRegex("a_b", '/') === expectedEscapedStrSeven)
    assert(escapeLikeRegex("a_b", '\"') === expectedEscapedStrSeven)
  }

  test("filter pattern") {
    val names = Seq("a1", "a2", "b2", "c3")
    assert(filterPattern(names, " * ") === Seq("a1", "a2", "b2", "c3"))
    assert(filterPattern(names, "*a*") === Seq("a1", "a2"))
    assert(filterPattern(names, " *a* ") === Seq("a1", "a2"))
    assert(filterPattern(names, " a* ") === Seq("a1", "a2"))
    assert(filterPattern(names, " a.* ") === Seq("a1", "a2"))
    assert(filterPattern(names, " B.*|a* ") === Seq("a1", "a2", "b2"))
    assert(filterPattern(names, " a. ") === Seq("a1", "a2"))
    assert(filterPattern(names, " d* ") === Nil)
  }
}
