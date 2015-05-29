package com.knime.bigdata.spark.testing.jobserver.client;

import static org.apache.spark.mllib.random.RandomRDDs.normalJavaRDD;
import static org.junit.Assert.assertEquals;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.api.java.Row;
import org.junit.Test;

import com.knime.bigdata.spark.jobserver.server.RDDUtils;
import com.knime.bigdata.spark.testing.jobserver.server.transformations.TestMultipleTransformations;

/**
 *
 * @author dwk
 *
 */
public class RDDUtilsTest  {

	private static final SparkConf conf = new SparkConf().setAppName(
			TestMultipleTransformations.class.getSimpleName()).setMaster(
			"local");

	private static class MyMapper implements Serializable {
		private static final long serialVersionUID = 1L;

		JavaRDD<Vector> apply(final JavaDoubleRDD o) {
			return o.map(new Function<Double, Vector>() {
				private static final long serialVersionUID = 1L;

		        @Override
				public Vector call(final Double x) {
					return Vectors.dense(x, 2.0 * x, x * x, x + 45);
				}
			});
		}

		JavaRDD<Row> toRowRdd(final JavaDoubleRDD o) {
			return o.map(new Function<Double, Row>() {
				private static final long serialVersionUID = 1L;

		        @Override
				public Row call(final Double x) {
					return Row.create(x, 2.0 * x, x * x, x + 45);
				}
			});
		}

	}

	@Test
	public void whatDoesRowCreateDo() {
		Row row = Row.create(new Object[]{"a", 1, 2.8d});
		assertEquals("should have three elements", 3, row.length());
	}

	@Test
	public void conversionOfJavaPairedRDD2JavaRDDWithRows() throws Throwable {
		try (final JavaSparkContext sparkContext = new JavaSparkContext(conf)) {
			// JavaRDD input1 = sparkContext.makeRDD();
			JavaDoubleRDD o = normalJavaRDD(sparkContext, 100L, 2);
			JavaRDD<Vector> v = new MyMapper().apply(o);
			JavaRDD<Row> rowRDD = RDDUtils.toJavaRDDOfRows(v.zip(o));

			assertEquals("conversion should keep number rows constant ",
					rowRDD.count(), 100);
			assertEquals("conversion should create correct length of rows ", 5,
					rowRDD.collect().get(0).length());

		}
	}

	@Test
	public void conversionOfJavaRowRDD2JavaRDDWithVector() throws Throwable {
		try (final JavaSparkContext sparkContext = new JavaSparkContext(conf)) {
			// JavaRDD input1 = sparkContext.makeRDD();
			JavaDoubleRDD o = normalJavaRDD(sparkContext, 100L, 2);
			JavaRDD<Row> v = new MyMapper().toRowRdd(o);
			JavaRDD<Vector> rowRDD = RDDUtils.toJavaRDDOfVectors(v);

			assertEquals("conversion should keep number rows constant ",
					rowRDD.count(), 100);
			assertEquals("conversion should create correct length of vectors ", 4,
					rowRDD.collect().get(0).size());

		}
	}

}