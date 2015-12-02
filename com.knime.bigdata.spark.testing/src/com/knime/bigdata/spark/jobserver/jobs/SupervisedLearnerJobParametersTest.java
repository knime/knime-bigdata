package com.knime.bigdata.spark.jobserver.jobs;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.NominalFeatureInfo;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.SupervisedLearnerUtils;
import com.knime.bigdata.spark.jobserver.server.EnumContainer.LinearLossFunctionTypeType;
import com.knime.bigdata.spark.jobserver.server.EnumContainer.LinearRegularizerType;
import com.knime.bigdata.spark.node.mllib.prediction.linear.LinearLearnerTask;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class SupervisedLearnerJobParametersTest {

	static String getParams(final String aTable,
			final NominalFeatureInfo aMappingTable, final Integer[] aColIdxs,
			final Integer aLabelIx, final String aResultTableName)
			throws GenericKnimeSparkException {

		return LinearLearnerTask.paramsAsJason(aTable,  aColIdxs,
				aLabelIx, 10, 0.5d, false, 5, 0.9d, LinearRegularizerType.L1, true, false, true,
				LinearLossFunctionTypeType.Logistic, 0.6, 0.9);
	}

	@Test
	public void jobValidationShouldCheckMissingInputDataParameter()
			throws Throwable {
		final String params = getParams(null, NominalFeatureInfo.EMPTY,
				new Integer[] { 0, 1 },  2,
				"OutTab");
		myCheck(params, KnimeSparkJob.PARAM_INPUT_TABLE, "Input");
	}

	@Test
	public void jobValidationShouldCheckMissingColumnSelectionParameter()
			throws Throwable {
		final String params = getParams("data", NominalFeatureInfo.EMPTY, null,
				 2, "OutTab");
		myCheck(params, "Input parameter '" + ParameterConstants.PARAM_COL_IDXS
				+ "' is not of expected type 'integer list'.");
	}

	@Test
	public void jobValidationShouldCheckMissingLabelIdxParameter()
			throws Throwable {
		final String params = getParams("data", NominalFeatureInfo.EMPTY,
				new Integer[] { 0, 1 },  null,
				"OutTab");
		myCheck(params, ParameterConstants.PARAM_LABEL_INDEX, "Input");
	}

	@Test
	public void jobValidationShouldShouldNotComplainAboutMissingButOptionalMappingParameter()
			throws Throwable {
		final String params = getParams("data", null,
				new Integer[] { 0, 1, 2 }, 3, "OutTab");
		myCheck(params, null);
	}

	@Test
	public void jobValidationShouldNotComplainAboutMissingButOptionalOuputParameter()
			throws Throwable {
		final String params = getParams("data", NominalFeatureInfo.EMPTY,
				new Integer[] { 0, 1 }, 2, null);
		myCheck(params, null);
	}

	static String allValidParams() throws GenericKnimeSparkException {
		return getParams("data", NominalFeatureInfo.EMPTY, new Integer[] { 0,
				1, 2 }, 3, "outtab");
	}

	@Test
	public void jobValidationShouldCheckAllValidParams() throws Throwable {
		final String params = allValidParams();
		final JobConfig config = new JobConfig(
				ConfigFactory.parseString(params));
		assertEquals("Configuration should be recognized as valid", null,
				SupervisedLearnerUtils.checkConfig(config));
	}

	private void myCheck(final String params, final String aParam,
			final String aPrefix) {
		myCheck(params, aPrefix + " parameter '" + aParam + "' missing.");
	}

	private void myCheck(final String params, final String aMsg) {
		final JobConfig config = new JobConfig(
				ConfigFactory.parseString(params));
		assertEquals("Configuration should be recognized as invalid", aMsg,
				SupervisedLearnerUtils.checkConfig(config));
	}

}