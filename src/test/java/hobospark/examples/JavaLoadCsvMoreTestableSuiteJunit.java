package hobospark.examples;

import spark.*;
import spark.api.java.JavaSparkContext;
import spark.api.java.JavaRDD;
import spark.api.java.JavaDoubleRDD;
import org.scalatest.FunSuite;
import org.scalatest.matchers.ShouldMatchers;

import static org.junit.Assert.assertEquals;
import org.junit.Test;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;


@RunWith(JUnit4.class)
public class JavaLoadCsvMoreTestableSuiteJunit {
    @Test
	public void testSumDataOnInput() {
	JavaSparkContext sc = new JavaSparkContext("local", "Load Java CSV test");
	Accumulator<Integer> counter = sc.intAccumulator(0);
	String[] inputArray = {"1,2","1,3","murh"};
	JavaRDD<String> input = sc.parallelize(Arrays.asList(inputArray));
	JavaDoubleRDD resultRDD = JavaLoadCsvMoreTestable.processData(counter, input);
	long resultCount = resultRDD.count();
	assertEquals(resultCount, 2);
	int errors = counter.value();
	assertEquals(errors, 1);
	sc.stop();
  }
}
