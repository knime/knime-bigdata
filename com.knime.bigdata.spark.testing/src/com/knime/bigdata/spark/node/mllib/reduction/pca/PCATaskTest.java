package com.knime.bigdata.spark.node.mllib.reduction.pca;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.knime.bigdata.spark.SparkWithJobServerSpec;
import com.knime.bigdata.spark.jobserver.client.KnimeContext;
import com.knime.bigdata.spark.jobserver.jobs.ImportKNIMETableJobTest;
import com.knime.bigdata.spark.jobserver.jobs.PCAJob;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class PCATaskTest extends SparkWithJobServerSpec {

	public static String paramsAsJason(final String aInputTableName,
			final Integer[] aNumericColIdx, final int aK, final String aMatrix, final String aPM) {
		return PCATask.paramsAsJason(aInputTableName, aNumericColIdx, aK,
				aMatrix, aPM);
	}

	@Test
	public void ensureThatAllRequiredParametersAreSet() throws Throwable {
		PCATask testObj = new PCATask(null, "inputRDD", new Integer[] { 0, 1 },
				2, "u", "p");
		final String params = testObj.paramsAsJason();
		JobConfig config = new JobConfig(ConfigFactory.parseString(params));

		assertEquals("Configuration should be recognized as valid",
				ValidationResultConverter.valid(),
				new PCAJob().validate(config));
	}

	@Test
	public void verifyThatPCAJobStoresResultMatrixAsNamedRDD()
			throws Throwable {
		KNIMESparkContext context = KnimeContext.getSparkContext();
		ImportKNIMETableJobTest.importTestTable(context, MINI_IRIS_TABLE,
				"tab1");

		final int k = 3;
		final Integer[] cols = new Integer[] { 0, 1, 2, 3 };
		PCATask testObj = new PCATask(context, "tab1", cols, k, "u", "p");

		testObj.execute(null);

		// not sure what else to check here....
		fetchResultTable(context, "u", 4);

	}

//	@Test
//	public void conversionOfColMajor2Dim() throws Throwable {
//		
//		final int nRows = 3;
//		final int nCols = 4;
//		final double[] dat = new double[] { 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7,
//				0.8, 0.9, 0.10, 0.11, 0.12 };
//		Matrix matrix = Matrices.dense(nRows, nCols, dat);
//
//		
//		final double[] vals = matrix.toArray();
//		final double[][] mat = PCATask.convertColumMajorArrayTo2Dim(vals, nCols);
//
//		assertEquals("num rows", nRows, mat.length);
//		assertEquals("num cols", nCols, mat[0].length);
//		assertEquals("mat[0,0]", 0.1, mat[0][0], 0.01);
//		assertEquals("mat[0,1]", 0.4, mat[0][1], 0.01);
//		assertEquals("mat[0,2]", 0.7, mat[0][2], 0.01);
//		assertEquals("mat[0,3]", 0.1, mat[0][3], 0.01);
//		assertEquals("mat[1,0]", 0.2, mat[1][0], 0.01);
//		assertEquals("mat[1,1]", 0.5, mat[1][1], 0.01);
//		assertEquals("mat[1,2]", 0.8, mat[1][2], 0.01);
//		assertEquals("mat[1,3]", 0.11, mat[1][3], 0.01);
//	}
	
}
