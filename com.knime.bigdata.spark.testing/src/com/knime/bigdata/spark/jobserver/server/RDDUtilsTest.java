package com.knime.bigdata.spark.jobserver.server;

import static org.apache.spark.mllib.random.RandomRDDs.normalJavaRDD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.api.java.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;

/**
 * 
 * @author dwk
 *
 */
public class RDDUtilsTest {
	private static class SparkContextResource extends ExternalResource {
		private static final SparkConf conf = new SparkConf().setAppName(
				RDDUtilsTest.class.getSimpleName()).setMaster("local");

		public JavaSparkContext sparkContext;

		@Override
		protected void before() {
			sparkContext = new JavaSparkContext(conf);
		}

		@Override
		protected void after() {
			sparkContext.close();
		}
	}

	@Rule
	public SparkContextResource sparkContextResource = new SparkContextResource();

	private static class MyMapper implements Serializable {
		private static final long serialVersionUID = 1L;

		JavaRDD<Vector> apply(JavaDoubleRDD o) {
			return o.map(new Function<Double, Vector>() {
				private static final long serialVersionUID = 1L;
				private int ix = 0;

				@Override
				public Vector call(Double x) {
					ix = ix + 1;
					return Vectors.dense(x, 2.0 * x, ix, x + 45);
				}
			});
		}

		JavaRDD<Row> toRowRdd(JavaDoubleRDD o) {
			return o.map(new Function<Double, Row>() {
				private static final long serialVersionUID = 1L;
				private int ix = 0;

				@Override
				public Row call(Double x) {
					ix = ix + 1;
					return Row.create(x, 2.0 * x, ix, x + 14);
				}
			});
		}

	}

	@Test
	public void getDoubleWithAllNumbers() {
		Row row = Row.create(new Object[] { 1d, 2d, 0.2d });

		assertEquals("elem at index 0", 1d, RDDUtils.getDouble(row, 0), 0.1d);
		assertEquals("elem at index 1", 2d, RDDUtils.getDouble(row, 1), 0.1d);
		assertEquals("elem at index 2 ", 0.2d, RDDUtils.getDouble(row, 2), 0.1d);
	}

	@Test(expected = IllegalArgumentException.class)
	public void getDoubleMustNotAcceptNullValues() {
		Row row = Row.create(new Object[] { 1d, 2d, null });

		assertEquals("elem at index 2 is null", 1d, RDDUtils.getDouble(row, 2),
				0.1d);
	}

	@Test(expected = IllegalArgumentException.class)
	public void getDoubleMustNotAcceptStringValues() {
		Row row = Row.create(new Object[] { "aa" });

		assertEquals("elem at index 0", 1d, RDDUtils.getDouble(row, 0), 0.1d);
	}

	@Test
	public void conversionOfJavaPairedRDD2JavaRDDWithRows() throws Exception {
		// JavaRDD input1 = sparkContext.makeRDD();
		JavaDoubleRDD o = normalJavaRDD(sparkContextResource.sparkContext,
				100L, 2);
		JavaRDD<Vector> v = new MyMapper().apply(o);
		JavaRDD<Row> rowRDD = RDDUtils.toJavaRDDOfRows(v.zip(o));

		assertEquals("conversion should keep number rows constant ",
				rowRDD.count(), 100);
		assertEquals("conversion should create correct length of rows ", 5,
				rowRDD.collect().get(0).length());
	}

	@Test
	public void conversionOfJavaRowRDD2JavaRDDWithVector() throws Exception {
		// JavaRDD input1 = sparkContext.makeRDD();
		JavaDoubleRDD o = normalJavaRDD(sparkContextResource.sparkContext,
				100L, 2);
		JavaRDD<Row> v = new MyMapper().toRowRdd(o);
		JavaRDD<Vector> rowRDD = RDDUtils.toJavaRDDOfVectors(v);

		assertEquals("conversion should keep number rows constant ",
				rowRDD.count(), 100);
		assertEquals("conversion should create correct length of vectors ", 4,
				rowRDD.collect().get(0).size());
	}

	@Test
	public void addColumn2JavaRowRDD() throws Exception {
		// JavaRDD input1 = sparkContext.makeRDD();
		JavaDoubleRDD o = normalJavaRDD(sparkContextResource.sparkContext,
				100L, 2);
		JavaRDD<Row> v = new MyMapper().toRowRdd(o);

		JavaRDD<Row> rowRDD = RDDUtils.addColumn(v.zip(o));

		assertEquals("conversion should keep number rows constant ",
				rowRDD.count(), 100);
		assertEquals("conversion should add single column ", 5, rowRDD
				.collect().get(0).length());
	}

	@Test
	public void conversionOfJavaRowRDD2JavaRDDWithVectorKeepOnlySomeFeatures()
			throws Exception {
		// JavaRDD input1 = sparkContext.makeRDD();
		JavaDoubleRDD o = normalJavaRDD(sparkContextResource.sparkContext,
				100L, 2);
		JavaRDD<Row> v = new MyMapper().toRowRdd(o);
		List<Integer> ix = new ArrayList<Integer>();
		ix.add(0);
		ix.add(1);
		ix.add(3);
		JavaRDD<Vector> rowRDD = RDDUtils.toJavaRDDOfVectorsOfSelectedIndices(
				v, ix);

		assertEquals("conversion should keep number rows constant ",
				rowRDD.count(), 100);
		assertEquals("conversion should create correct length of vectors ", 3,
				rowRDD.collect().get(0).size());

	}

	@Test
	public void conversionOfJavaRowRDD2JavaRDDWithLabeledPoint()
			throws Exception {
		// JavaRDD input1 = sparkContext.makeRDD();
		JavaDoubleRDD o = normalJavaRDD(sparkContextResource.sparkContext,
				100L, 1);
		JavaRDD<Row> v = new MyMapper().toRowRdd(o);
		JavaRDD<LabeledPoint> rowRDD = RDDUtils.toJavaLabeledPointRDD(v, 2);

		assertEquals("conversion should keep number rows constant ",
				rowRDD.count(), 100);

		List<LabeledPoint> rows = rowRDD.collect();
		for (int i = 0; i < rows.size(); i++) {
			double[] features = rows.get(i).features().toArray();
			assertEquals("conversion should create correct length of vectors ",
					3, features.length);
			for (int j = 0; j < features.length; j++) {
				assertTrue("label should not be contained in features",
						Math.abs(features[j] - i) > 0.0001);
			}
			assertEquals("conversion should set proper label ", 1 + i,
					(int) rows.get(i).label());
		}
	}

	/**
	 * Test that {@link RDDUtils} creates a {@link LabeledPoint} RDD containing
	 * the columns with the given array of indexes in their intrinsic order.
	 * 
	 * @throws Exception
	 */
	@Test
	public void createsALabeledPointRDDContainingTheColumnsWithTheGivenArrayOfIndexesInTheirIntrinsicOrder()
			throws Exception {
		final Object[] rowValues = { Byte.valueOf((byte) 1), Float.valueOf(2f),
				Long.valueOf(3L), Integer.valueOf(4), Double.valueOf(5),
				Short.valueOf((short) 6) };

		final List<Row> rows = repeatRowValues(100, rowValues);
		final JavaRDD<Row> inputRDD = sparkContextResource.sparkContext
				.parallelize(rows);

		final int labelColumnIndex = 2;
		final int[] columnIndexes = { 5, 0, 4, 2 };

		final List<LabeledPoint> convertedRDD = RDDUtils.toJavaLabeledPointRDD(
				inputRDD, labelColumnIndex, columnIndexes).collect();

		for (LabeledPoint p : convertedRDD) {
			assertTrue(hasFeatureVector(p, 6.0, 1.0, 5.0, 3.0));
		}
	}

	@Nonnull
	private static List<Row> repeatRowValues(final int numberOfRows,
			Object... rowValues) {
		assert numberOfRows > 0;
		assert rowValues != null;

		final List<Row> rows = new ArrayList<>(numberOfRows);
		for (int i = 0; i < numberOfRows; ++i) {
			rows.add(Row.create(rowValues));
		}

		return rows;
	}

	/**
	 * Test that {@link RDDUtils} creates a {@link LabeledPoint} RDD containing
	 * the columns with the given list of indexes in their intrinsic order.
	 * 
	 * @throws Exception
	 */
	@Test
	public void createsALabeledPointRDDContaininTheColumnsWithTheGivenListOfIndexesInTheirIntrinsicOrder()
			throws Exception {
		final Object[] rowValues = { Byte.valueOf((byte) 1), Float.valueOf(2f),
				Long.valueOf(3L), Integer.valueOf(4), Double.valueOf(5),
				Short.valueOf((short) 6) };

		final List<Row> rows = repeatRowValues(100, rowValues);
		final JavaRDD<Row> inputRDD = sparkContextResource.sparkContext
				.parallelize(rows);

		final int labelColumnIndex = 0;
		final List<Integer> columnIndexes = Arrays.asList(3, 1, 5, 4);

		final List<LabeledPoint> convertedRDD = RDDUtils.toJavaLabeledPointRDD(
				inputRDD, labelColumnIndex, columnIndexes).collect();

		for (LabeledPoint p : convertedRDD) {
			assertTrue(hasFeatureVector(p, 4.0, 2.0, 6.0, 5.0));
		}
	}

	@Nonnull
	private static boolean hasFeatureVector(final LabeledPoint labeledPoint,
			final double... expectedFeatures) {
		assert expectedFeatures != null;

		final double[] actualFeatures = labeledPoint.features().toArray();

		assertEquals(actualFeatures.length, expectedFeatures.length);
		for (int i = 0; i < actualFeatures.length; ++i) {
			assertEquals(expectedFeatures[i], actualFeatures[i], 0.001d);
		}

		return true;
	}

}