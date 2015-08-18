package com.knime.bigdata.spark.jobserver.jobs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.knime.base.node.preproc.sample.SamplingNodeSettings;
import org.knime.base.node.preproc.sample.SamplingNodeSettings.CountMethods;
import org.knime.base.node.preproc.sample.SamplingNodeSettings.SamplingMethods;

import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.node.mllib.sampling.MLlibSamplingNodeModel;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 * (these tests do not need a running job-server)
 *
 */
@SuppressWarnings("javadoc")
public class SamplingJobParametersTest {
	private static String getParams(final String aTableToSample,
			final SamplingMethods aMethod, final CountMethods aCountMethod,
			final boolean aIsWithReplacement, final String aOut1, final String aOut2) {
		return getParams(aTableToSample, aMethod, aCountMethod,
				aIsWithReplacement, false, 99l, 0.6d, 77777, aOut1, aOut2);
	}

	static String getParams(final String aTableToSample,
			final SamplingMethods aMethod, final CountMethods aCountMethod,
			final boolean aIsWithReplacement, final boolean aExact, final Long aSeed, final double aFraction, final int aAbsoluteCount, final String aOut1,
			final String aOut2) {
		final SamplingNodeSettings settings = new SamplingNodeSettings();
		settings.samplingMethod(aMethod);
		settings.countMethod(aCountMethod);
		settings.count(aAbsoluteCount);
		settings.fraction(aFraction);
		return MLlibSamplingNodeModel.paramDef(aTableToSample, settings, -1,
				aIsWithReplacement, aSeed, aExact, aOut1, aOut2);
	}

	@Test
	public void jobValidationShouldCheckMissingInputTableParameter()
			throws Throwable {
		final String params = getParams(null, SamplingMethods.First,
				CountMethods.Absolute, false, null, null);
		myCheck(params, ParameterConstants.PARAM_TABLE_1, "Input");
	}

	@Test(expected = NullPointerException.class)
	public void jobValidationShouldFailForMissingSettingsParameter()
			throws Throwable {
		final String params = MLlibSamplingNodeModel.paramDef("tab", null, -1, false, 789,true,
				null, null);
		myCheck(params, ParameterConstants.PARAM_TABLE_1, "Input");
	}

	@Test(expected = NullPointerException.class)
	public void jobValidationShouldCheckMissingCountMethodParameter()
			throws Throwable {
		final String params = getParams("tab", SamplingMethods.First, null, false,
				null, null);
		myCheck(params, SamplingJob.PARAM_COUNT_METHOD, "Input");
	}

	@Test(expected = NullPointerException.class)
	public void jobValidationShouldCheckMissingSamplingMethodParameter()
			throws Throwable {
		final String params = getParams("tab", null, CountMethods.Absolute, false,
				null, null);
		myCheck(params, SamplingJob.PARAM_SAMPLING_METHOD, "Input");
	}

	@Test
	public void jobValidationShouldCheckMissingResultTableParameter()
			throws Throwable {
		final String params = getParams("tab", SamplingMethods.First,
				CountMethods.Absolute, false, null, null);
		myCheck(params, KnimeSparkJob.PARAM_RESULT_TABLE, "Output");
	}

	@Test
	public void checkIsSplitOp() throws Throwable {
		final String params = getParams("tab", SamplingMethods.First,
				CountMethods.Absolute, false, "out1", "out2");
		final JobConfig config = new JobConfig(ConfigFactory.parseString(params));
		assertTrue("split job expected", SamplingJob.isSplit(config));
	}

	@Test
	public void checkIsSampleOp() throws Throwable {
		final String params = getParams("tab", SamplingMethods.First,
				CountMethods.Absolute, false, "out1", null);
		final JobConfig config = new JobConfig(ConfigFactory.parseString(params));
		assertFalse("sample job expected", SamplingJob.isSplit(config));
	}

	@Test
	public void checkIsWithReplacement() throws Throwable {
		final String params = getParams("tab", SamplingMethods.First,
				CountMethods.Absolute, true, "out1", null);
		final JobConfig config = new JobConfig(ConfigFactory.parseString(params));
		assertTrue("sampling with replacement expected",
				SamplingJob.getWithReplacement(config));
	}

	@Test
	public void checkIsWithoutReplacement() throws Throwable {
		final String params = getParams("tab", SamplingMethods.First,
				CountMethods.Absolute, false, "out1", null);
		final JobConfig config = new JobConfig(ConfigFactory.parseString(params));
		assertFalse("sampling without replacement expected",
				SamplingJob.getWithReplacement(config));
	}

	@Test
	public void checkSeedParameter() throws Throwable {
		final String params = getParams("tab", SamplingMethods.First,
				CountMethods.Absolute, false,  true,4657l,0.77d, 55, "out1", null);
		final JobConfig config = new JobConfig(ConfigFactory.parseString(params));
		assertEquals("sampling seed", 4657l, SamplingJob.getSeed(config));
	}

	@Test
	public void checkFractionParameter() throws Throwable {
		final String params = getParams("tab", SamplingMethods.First,
				CountMethods.Absolute, false,true, 4657l, 0.787d, 44,  "out1", null);
		final JobConfig config = new JobConfig(ConfigFactory.parseString(params));
		assertEquals("sampling fraction", 0.787d, SamplingJob.getFraction(config), 0.000001);
	}

	@Test
	public void checkAbsoluteNumberParameter() throws Throwable {
		final String params = getParams("tab", SamplingMethods.First,
				CountMethods.Absolute, false, true,4657l, 0.787d, 38514689, "out1", null);
		final JobConfig config = new JobConfig(ConfigFactory.parseString(params));
		assertEquals("sampling absolute count", 38514689l, SamplingJob.getCount(config));
	}

	private void myCheck(final String params, final String aParam,
			final String aPrefix) {
		myCheck(params, aPrefix + " parameter '" + aParam + "' missing.");
	}

	private void myCheck(final String params, final String aMsg) {
		final KnimeSparkJob testObj = new SamplingJob();
		final JobConfig config = new JobConfig(ConfigFactory.parseString(params));
		assertEquals("Configuration should be recognized as invalid",
				ValidationResultConverter.invalid(aMsg),
				testObj.validate(config));
	}

}