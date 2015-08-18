package com.knime.bigdata.spark.jobserver.jobs;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.knime.bigdata.spark.SparkWithJobServerSpec;
import com.knime.bigdata.spark.jobserver.jobs.ConvertNominalValuesJob;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.MappingType;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.node.preproc.convert.stringmapper.ValueConverterTask;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class SparkStringMapperJobTest extends SparkWithJobServerSpec {

    private static String getParams(final String aInputDataPath, final String aType, final Integer[] aColIdxes,
        final String[] aColNames, final String aOutputDataPath1, final String aOutputDataPath2) {
        return ValueConverterTask.paramDef(aColIdxes, aColNames, aType, aInputDataPath, aOutputDataPath1,
            aOutputDataPath2);
    }

    @Test
    public void jobValidationShouldCheckMissingInputDataParameter() throws Throwable {
        String params =
            getParams(null, MappingType.COLUMN.toString(), new Integer[]{1, 5, 2, 7}, new String[]{"a", "b", "c", "d"},
                "tab1", "tab2");
        myCheck(params, ConvertNominalValuesJob.PARAM_INPUT_TABLE, "Input");
    }

    @Test
    public void jobValidationShouldCheckMissingMappingTypeParameter() throws Throwable {
        String params = getParams("xx", null, new Integer[]{9}, new String[]{"a", "b", "c", "d"}, "tab1", "tab2");
        myCheck(params, ConvertNominalValuesJob.PARAM_MAPPING_TYPE, "Input");
    }

    @Test
    public void jobValidationShouldCheckIncorrectMappingTypeParameter() throws Throwable {
        String params =
            getParams("xx", "notproper", new Integer[]{99}, new String[]{"a", "b", "c", "d"}, "tab1", "tab2");
        String msg = "Input parameter '" + ConvertNominalValuesJob.PARAM_MAPPING_TYPE + "' has an invalid value.";
        myCheck(params, msg);
    }

    @Test
    public void jobValidationShouldCheckMissingColSelectionParameter() throws Throwable {
        String params =
            getParams("tab1", MappingType.COLUMN.toString(), null, new String[]{"a", "b", "c", "d"}, "tab1", "tab2");
        myCheck(params, "Input parameter '" + ParameterConstants.PARAM_COL_IDXS
            + "' is not of expected type 'integer list'.");
    }

    //not sure whether we really want to disallow empty column selection...
    //@Test
    public void jobValidationShouldCheckIncorrectColSelectionParameter() throws Throwable {
        String params =
            getParams("tab1", MappingType.COLUMN.toString(), new Integer[]{}, new String[]{"a", "b", "c", "d"}, "tab1",
                "tab2");
        String msg = "Input parameter '" + ParameterConstants.PARAM_COL_IDXS + "' is empty.";
        myCheck(params, msg);

    }

    @Test
    public void jobValidationShouldCheckMissingOuputParameter1() throws Throwable {
        String params =
            getParams("tab1", MappingType.COLUMN.toString(), new Integer[]{1, 5, 2}, new String[]{"a", "b", "c", "d"},
                null, "tab2");
        myCheck(params, ConvertNominalValuesJob.PARAM_RESULT_TABLE, "Output");

    }

    @Test
    public void jobValidationShouldCheckMissingOuputParameter2() throws Throwable {
        String params =
            getParams("tab1", MappingType.COLUMN.toString(), new Integer[]{1, 5, 2}, new String[]{"a", "b", "c", "d"},
                "tab1", null);
        myCheck(params, ConvertNominalValuesJob.PARAM_RESULT_MAPPING, "Output");

    }

    private void myCheck(final String params, final String aParam, final String aPrefix) {
        myCheck(params, aPrefix + " parameter '" + aParam + "' missing.");
    }

    private void myCheck(final String params, final String aMsg) {
        ConvertNominalValuesJob testObj = new ConvertNominalValuesJob();
        JobConfig config = new JobConfig(ConfigFactory.parseString(params));
        assertEquals("Configuration should be recognized as invalid", ValidationResultConverter.invalid(aMsg),
            testObj.validate(config));
    }
}