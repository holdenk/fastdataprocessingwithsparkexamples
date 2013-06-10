package hobospark.examples

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers

class JavaLoadCsvExampleSuite extends FunSuite with ShouldMatchers {

    test("should parse a csv line with numbers") {
      val parseLine = new JavaLoadCsvTestable.ParseLine();
      parseLine.call("1,2") should equal (Array[Integer](1,2))
      parseLine.call("100,-1,1,2,2") should equal (Array[Integer](100,-1,1,2,2))
    }
    test("should error if there is a non-integer") {
      val parseLine = new JavaLoadCsvTestable.ParseLine();
      evaluating { parseLine.call("pandas")  } should produce [NumberFormatException]
      evaluating {parseLine.call("100,-1,1,2.2,2") should equal (Array[Integer](100,-1,1,2,2)) } should produce [NumberFormatException]
    }
}
