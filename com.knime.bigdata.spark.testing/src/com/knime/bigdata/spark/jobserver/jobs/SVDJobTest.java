package com.knime.bigdata.spark.jobserver.jobs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.SingularValueDecomposition;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.sql.api.java.Row;
import org.junit.Test;

import com.knime.bigdata.spark.LocalSparkSpec;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.node.mllib.reduction.svd.SVDTaskTest;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class SVDJobTest extends LocalSparkSpec {

	@Test
	public void jobValidationShouldCheckMissingInputDataParameter()
			throws Throwable {
		String params = SVDTaskTest.paramsAsJason(null, new Integer[] { 0, 1 },
				true, 8, 0.001, "V", "U");
		myCheck(params, KnimeSparkJob.PARAM_INPUT_TABLE, "Input");
	}

	@Test
	public void jobValidationShouldCheckComputeUParameter() throws Throwable {
		for (boolean b : new boolean[] { true, false }) {
			String params = SVDTaskTest.paramsAsJason("tab1",
					new Integer[] { 0, 1 }, b, 7, 0.5, "V", "U");
			Config config = ConfigFactory.parseString(params);
			JobConfig config2 = new JobConfig(config);
			assertEquals("compute u parameter not set", b,
					SVDJob.getComputeU(config2));
		}
	}

	@Test
	public void jobValidationShouldCheckKParameter() throws Throwable {
		for (Integer k : new int[] { -1, 0, 1, 3 }) {
			String params = SVDTaskTest.paramsAsJason("tab1",
					new Integer[] { 0, 1 }, true, k, 0.5, "V", "U");
			Config config = ConfigFactory.parseString(params);
			JobConfig config2 = new JobConfig(config);
			assertEquals("'k' parameter not set", k, SVDJob.getK(config2));
		}
	}

	@Test
	public void jobValidationShouldCheckRCondParameter() throws Throwable {
		for (Double r : new double[] { -0.1d, 0d, .1d, 3d }) {
			String params = SVDTaskTest.paramsAsJason("tab1",
					new Integer[] { 0, 1 }, true, 5, r, "V", "U");
			Config config = ConfigFactory.parseString(params);
			JobConfig config2 = new JobConfig(config);
			assertEquals("'k' parameter not set", r, SVDJob.getRCond(config2),
					0.00001);
		}
	}

	@Test
	public void jobValidationShouldCheckAllValidParams() throws Throwable {
		String params = SVDTaskTest.paramsAsJason("tab1", new Integer[] { 0, 1 },
				true, 5, 0.3, "V", "U");
		KnimeSparkJob testObj = new SVDJob();
		Config config = ConfigFactory.parseString(params);
		JobConfig config2 = new JobConfig(config);
		assertEquals("Configuration should be recognized as valid",
				ValidationResultConverter.valid(), testObj.validate(config2));
	}

	private static class MyMapper implements Serializable {
		private static final long serialVersionUID = 1L;

		private JavaRDD<Row> getTestRdd(JavaDoubleRDD o) {
			return o.map(new Function<Double, Row>() {
				private static final long serialVersionUID = 1L;
				private int ix = 0;

				@Override
				public Row call(final Double x) {
					ix = ix + 1;
					return Row.create(x, 2.0 * x, ix, x + 14);
				}
			});
		}
	}

	@Test
	public void decompositionOfSimpleRowRDD() throws Throwable {
		JavaDoubleRDD o = getRandomDoubleRDD(100, 2);
		final int k = 2;
		String params = SVDTaskTest.paramsAsJason("tab1", new Integer[] { 0, 1 },
				true, k, 0.3, "V", "U");
		Config config = ConfigFactory.parseString(params);
		JobConfig config2 = new JobConfig(config);
		SingularValueDecomposition<RowMatrix, Matrix> svd = SVDJob
				.decomposeRowRdd(config2, new MyMapper().getTestRdd(o));

		assertTrue("U should be computed", svd.U() != null);
		assertTrue("V should be computed", svd.V() != null);
		assertTrue("s should be computed", svd.s() != null);
		assertTrue("s should be computed and have length at most 'k'", k >= svd.s().size());		
	}

	private void myCheck(final String params, final String aParam,
			final String aPrefix) {
		myCheck(params, aPrefix + " parameter '" + aParam + "' missing.");
	}

	private void myCheck(final String params, final String aMsg) {
		KnimeSparkJob testObj = new SVDJob();
		JobConfig config = new JobConfig(ConfigFactory.parseString(params));
		assertEquals("Configuration should be recognized as invalid",
				ValidationResultConverter.invalid(aMsg),
				testObj.validate(config));
	}

}