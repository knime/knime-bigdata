package com.knime.bigdata.spark.jobserver.jobs;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.knime.bigdata.spark.SparkWithJobServerSpec;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.MappingType;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.node.preproc.convert.category2number.Category2NumberConverterTask;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class ConvertNominalValuesJobTest extends SparkWithJobServerSpec {

    private static String getParams(final String aInputDataPath, final String aType, final Integer[] aColIdxes,
        final String[] aColNames, final String aOutputDataPath1) {
        return Category2NumberConverterTask.paramDef(aColIdxes, aColNames, aType, aInputDataPath, aOutputDataPath1, true);
    }

    @Test
    public void jobValidationShouldCheckMissingInputDataParameter() throws Throwable {
        final String params =
            getParams(null, MappingType.COLUMN.toString(), new Integer[]{1, 5, 2, 7}, new String[]{"a", "b", "c", "d"},
                "tab1");
        myCheck(params, ConvertNominalValuesJob.PARAM_INPUT_TABLE, "Input");
    }

    @Test
    public void jobValidationShouldCheckMissingMappingTypeParameter() throws Throwable {
        final String params = getParams("xx", null, new Integer[]{9}, new String[]{"a", "b", "c", "d"}, "tab1");
        myCheck(params, ConvertNominalValuesJob.PARAM_MAPPING_TYPE, "Input");
    }

    @Test
    public void jobValidationShouldCheckIncorrectMappingTypeParameter() throws Throwable {
        final String params =
            getParams("xx", "notproper", new Integer[]{99}, new String[]{"a", "b", "c", "d"}, "tab1");
        final String msg = "Input parameter '" + ConvertNominalValuesJob.PARAM_MAPPING_TYPE + "' has an invalid value.";
        myCheck(params, msg);
    }

    @Test
    public void jobValidationShouldCheckMissingColSelectionParameter() throws Throwable {
        final String params =
            getParams("tab1", MappingType.COLUMN.toString(), null, new String[]{"a", "b", "c", "d"}, "tab1");
        myCheck(params, "Input parameter '" + ParameterConstants.PARAM_COL_IDXS
            + "' is not of expected type 'integer list'.");
    }

    //not sure whether we really want to disallow empty column selection...
    //@Test
    public void jobValidationShouldCheckIncorrectColSelectionParameter() throws Throwable {
        final String params =
            getParams("tab1", MappingType.COLUMN.toString(), new Integer[]{}, new String[]{"a", "b", "c", "d"}, "tab1");
        final String msg = "Input parameter '" + ParameterConstants.PARAM_COL_IDXS + "' is empty.";
        myCheck(params, msg);

    }

    @Test
    public void jobValidationShouldCheckMissingOuputParameter1() throws Throwable {
        final String params =
            getParams("tab1", MappingType.COLUMN.toString(), new Integer[]{1, 5, 2}, new String[]{"a", "b", "c", "d"},
                null);
        myCheck(params, ConvertNominalValuesJob.PARAM_RESULT_TABLE, "Output");

    }

    private void myCheck(final String params, final String aParam, final String aPrefix) {
        myCheck(params, aPrefix + " parameter '" + aParam + "' missing.");
    }

    private void myCheck(final String params, final String aMsg) {
        final ConvertNominalValuesJob testObj = new ConvertNominalValuesJob();
        final JobConfig config = new JobConfig(ConfigFactory.parseString(params));
        assertEquals("Configuration should be recognized as invalid", ValidationResultConverter.invalid(aMsg),
            testObj.validate(config));
    }
}