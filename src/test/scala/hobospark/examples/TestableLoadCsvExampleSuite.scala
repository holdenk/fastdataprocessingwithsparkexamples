package hobospark.examples

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers

class TestableLoadCsvExampleSuite extends FunSuite with ShouldMatchers {
    test("should parse a csv line with numbers") {
      TestableLoadCsvExample.parseLine("1,2") should equal (Array[Double](1.0,2.0))
      TestableLoadCsvExample.parseLine("100,-1,1,2,2.5") should equal (Array[Double](100,-1,1.0,2.0,2.5))
    }
    test("should error if there is a non-number") {
      evaluating { TestableLoadCsvExample.parseLine("pandas")  } should produce [NumberFormatException]
    }
}
