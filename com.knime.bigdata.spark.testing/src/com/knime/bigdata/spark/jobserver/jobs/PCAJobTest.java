package com.knime.bigdata.spark.jobserver.jobs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.sql.api.java.Row;
import org.junit.Test;

import scala.Tuple2;

import com.knime.bigdata.spark.LocalSparkSpec;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.node.mllib.reduction.pca.PCATaskTest;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class PCAJobTest extends LocalSparkSpec {

	@Test
	public void jobValidationShouldCheckMissingInputDataParameter()
			throws Throwable {
		String params = PCATaskTest.paramsAsJason(null, new Integer[] { 0, 1 },
				8, "U", "P");
		myCheck(params, KnimeSparkJob.PARAM_INPUT_TABLE, "Input");
	}

	@Test
	public void jobValidationShouldCheckKParameter() throws Throwable {
		for (Integer k : new int[] { -1, 0, 1, 3 }) {
			String params = PCATaskTest.paramsAsJason("tab1", new Integer[] {
					0, 1 }, k, "U", "P");
			Config config = ConfigFactory.parseString(params);
			JobConfig config2 = new JobConfig(config);
			assertEquals("'k' parameter not set", k, PCAJob.getK(config2));
		}
	}

	@Test
	public void jobValidationShouldCheckAllValidParams() throws Throwable {
		String params = PCATaskTest.paramsAsJason("tab1",
				new Integer[] { 0, 1 }, 5, "U", "P");
		KnimeSparkJob testObj = new PCAJob();
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
	public void pcaOfSimpleRowRDD() throws Throwable {
		JavaDoubleRDD o = getRandomDoubleRDD(100, 2);
		final int k = 2;
		String params = PCATaskTest.paramsAsJason("tab1",
				new Integer[] { 0, 1 }, k, "U", "P");
		Config config = ConfigFactory.parseString(params);
		JobConfig config2 = new JobConfig(config);
		Tuple2<RowMatrix, Matrix> pca = PCAJob.compute(config2, new MyMapper().getTestRdd(o));

		assertTrue("principal components should be computed", pca != null);
		assertTrue("principal component matrix should be computed", pca._2 != null);
		assertEquals("there should be 'k' principal components ", k,
				pca._2.numRows());
	}


	private void myCheck(final String params, final String aParam,
			final String aPrefix) {
		myCheck(params, aPrefix + " parameter '" + aParam + "' missing.");
	}

	private void myCheck(final String params, final String aMsg) {
		KnimeSparkJob testObj = new PCAJob();
		JobConfig config = new JobConfig(ConfigFactory.parseString(params));
		assertEquals("Configuration should be recognized as invalid",
				ValidationResultConverter.invalid(aMsg),
				testObj.validate(config));
	}

}