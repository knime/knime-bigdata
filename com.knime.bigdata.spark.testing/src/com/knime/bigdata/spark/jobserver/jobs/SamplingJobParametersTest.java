package com.knime.bigdata.spark.jobserver.jobs;

import static org.junit.Assert.*;

import org.junit.Test;
import org.knime.base.node.preproc.sample.SamplingNodeSettings;
import org.knime.base.node.preproc.sample.SamplingNodeSettings.CountMethods;
import org.knime.base.node.preproc.sample.SamplingNodeSettings.SamplingMethods;

import com.knime.bigdata.spark.jobserver.jobs.SamplingJob;
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
			boolean aIsWithReplacement, String aOut1, String aOut2) {
		return getParams(aTableToSample, aMethod, aCountMethod,
				aIsWithReplacement, false, 99l, 0.6d, 77777, aOut1, aOut2);
	}

	private static String getParams(final String aTableToSample,
			final SamplingMethods aMethod, final CountMethods aCountMethod,
			boolean aIsWithReplacement, boolean aExact, final Long aSeed, double aFraction, int aAbsoluteCount, String aOut1,
			String aOut2) {
		SamplingNodeSettings settings = new SamplingNodeSettings();
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
		String params = getParams(null, SamplingMethods.First,
				CountMethods.Absolute, false, null, null);
		myCheck(params, ParameterConstants.PARAM_TABLE_1, "Input");
	}

	@Test(expected = NullPointerException.class)
	public void jobValidationShouldFailForMissingSettingsParameter()
			throws Throwable {
		String params = MLlibSamplingNodeModel.paramDef("tab", null, -1, false, 789,true,
				null, null);
		myCheck(params, ParameterConstants.PARAM_TABLE_1, "Input");
	}

	@Test(expected = NullPointerException.class)
	public void jobValidationShouldCheckMissingCountMethodParameter()
			throws Throwable {
		String params = getParams("tab", SamplingMethods.First, null, false,
				null, null);
		myCheck(params, SamplingJob.PARAM_COUNT_METHOD, "Input");
	}

	@Test(expected = NullPointerException.class)
	public void jobValidationShouldCheckMissingSamplingMethodParameter()
			throws Throwable {
		String params = getParams("tab", null, CountMethods.Absolute, false,
				null, null);
		myCheck(params, SamplingJob.PARAM_SAMPLING_METHOD, "Input");
	}

	@Test
	public void jobValidationShouldCheckMissingResultTableParameter()
			throws Throwable {
		String params = getParams("tab", SamplingMethods.First,
				CountMethods.Absolute, false, null, null);
		myCheck(params, KnimeSparkJob.PARAM_RESULT_TABLE, "Output");
	}

	@Test
	public void checkIsSplitOp() throws Throwable {
		String params = getParams("tab", SamplingMethods.First,
				CountMethods.Absolute, false, "out1", "out2");
		JobConfig config = new JobConfig(ConfigFactory.parseString(params));
		assertTrue("split job expected", SamplingJob.isSplit(config));
	}

	@Test
	public void checkIsSampleOp() throws Throwable {
		String params = getParams("tab", SamplingMethods.First,
				CountMethods.Absolute, false, "out1", null);
		JobConfig config = new JobConfig(ConfigFactory.parseString(params));
		assertFalse("sample job expected", SamplingJob.isSplit(config));
	}

	@Test
	public void checkIsWithReplacement() throws Throwable {
		String params = getParams("tab", SamplingMethods.First,
				CountMethods.Absolute, true, "out1", null);
		JobConfig config = new JobConfig(ConfigFactory.parseString(params));
		assertTrue("sampling with replacement expected",
				SamplingJob.getWithReplacement(config));
	}

	@Test
	public void checkIsWithoutReplacement() throws Throwable {
		String params = getParams("tab", SamplingMethods.First,
				CountMethods.Absolute, false, "out1", null);
		JobConfig config = new JobConfig(ConfigFactory.parseString(params));
		assertFalse("sampling without replacement expected",
				SamplingJob.getWithReplacement(config));
	}

	@Test
	public void checkSeedParameter() throws Throwable {
		String params = getParams("tab", SamplingMethods.First,
				CountMethods.Absolute, false,  true,4657l,0.77d, 55, "out1", null);
		JobConfig config = new JobConfig(ConfigFactory.parseString(params));
		assertEquals("sampling seed", 4657l, SamplingJob.getSeed(config));
	}

	@Test
	public void checkFractionParameter() throws Throwable {
		String params = getParams("tab", SamplingMethods.First,
				CountMethods.Absolute, false,true, 4657l, 0.787d, 44,  "out1", null);
		JobConfig config = new JobConfig(ConfigFactory.parseString(params));
		assertEquals("sampling fraction", 0.787d, SamplingJob.getFraction(config), 0.000001);
	}

	@Test
	public void checkAbsoluteNumberParameter() throws Throwable {
		String params = getParams("tab", SamplingMethods.First,
				CountMethods.Absolute, false, true,4657l, 0.787d, 38514689, "out1", null);
		JobConfig config = new JobConfig(ConfigFactory.parseString(params));
		assertEquals("sampling absolute count", 38514689l, SamplingJob.getCount(config));
	}

	private void myCheck(final String params, final String aParam,
			final String aPrefix) {
		myCheck(params, aPrefix + " parameter '" + aParam + "' missing.");
	}

	private void myCheck(final String params, final String aMsg) {
		KnimeSparkJob testObj = new SamplingJob();
		JobConfig config = new JobConfig(ConfigFactory.parseString(params));
		assertEquals("Configuration should be recognized as invalid",
				ValidationResultConverter.invalid(aMsg),
				testObj.validate(config));
	}

}