package com.knime.bigdata.spark.testing.jobserver.jobs;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.SupervisedLearnerUtils;
import com.knime.bigdata.spark.node.mllib.prediction.linear.SGDLearnerTask;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class SupervisedLearnerJobParametersTest {

    static String getParams(final String aTable, final String aMappingTable, final Integer[] aColIdxs,
        final String[] aColNames, final Integer aLabelIx, final String aResultTableName) {

        return SGDLearnerTask.learnerDef(aTable, aMappingTable, aColNames, aColIdxs, aLabelIx, 10, 0.5d);
    }

    @Test
    public void jobValidationShouldCheckMissingInputDataParameter() throws Throwable {
        String params = getParams(null, "mapping", new Integer[]{0, 1}, new String[]{"a", "b", "c"}, 2, "OutTab");
        myCheck(params, ParameterConstants.PARAM_INPUT + "." + ParameterConstants.PARAM_TABLE_1, "Input");
    }

    @Test
    public void jobValidationShouldCheckMissingColumnSelectionParameter() throws Throwable {
        String params = getParams("data", "mapping", null, new String[]{"a", "b", "c"}, 2, "OutTab");
        myCheck(params, "Input parameter '" + ParameterConstants.PARAM_INPUT + "." + ParameterConstants.PARAM_COL_IDXS
            + "' is not of expected type 'integer list'.");
    }

    @Test
    public void jobValidationShouldCheckMissingColumnNamesParameter() throws Throwable {
        String params = getParams("data", "mapping", new Integer[]{0, 1}, null, 2, "OutTab");
        myCheck(params, "Input parameter '" + ParameterConstants.PARAM_INPUT + "." + ParameterConstants.PARAM_COL_IDXS
            + ParameterConstants.PARAM_STRING + "' is not of expected type 'string list'.");
    }

    @Test
    public void jobValidationShouldComplainAboutIncompatibleNamesAndIndicesParameter() throws Throwable {
        String params = getParams("data", "mapping", new Integer[]{0, 1}, new String[]{"a", "b"}, 2, "OutTab");
        myCheck(
            params,
            "Input parameter '"
                + ParameterConstants.PARAM_INPUT
                + "."
                + ParameterConstants.PARAM_COL_IDXS
                + ParameterConstants.PARAM_STRING
                + "' is of unexpected length. It must have one entry for each select input column and 1 for the label column.");
    }

    @Test
    public void jobValidationShouldCheckMissingLabelIdxParameter() throws Throwable {
        String params = getParams("data", "mapping", new Integer[]{0, 1}, new String[]{"a", "b", "c"}, null, "OutTab");
        myCheck(params, ParameterConstants.PARAM_INPUT + "." + ParameterConstants.PARAM_LABEL_INDEX, "Input");
    }

    @Test
    public void jobValidationShouldShouldNotComplainAboutMissingButOptionalMappingParameter() throws Throwable {
        String params =
            getParams("data", null, new Integer[]{0, 1, 2}, new String[]{"a", "b", "c", "label"}, 3, "OutTab");
        myCheck(params, null);
    }

    @Test
    public void jobValidationShouldNotComplainAboutMissingButOptionalOuputParameter() throws Throwable {
        String params = getParams("data", "mapping", new Integer[]{0, 1}, new String[]{"a", "b", "c"}, 2, null);
        myCheck(params, null);
    }

    static String allValidParams() {
        return getParams("data", "mapping", new Integer[]{0, 1, 2}, new String[]{"a", "b", "c", "label"}, 3, "outtab");
    }

    @Test
    public void jobValidationShouldCheckAllValidParams() throws Throwable {
        String params = allValidParams();
        Config config = ConfigFactory.parseString(params);
        assertEquals("Configuration should be recognized as valid", null, SupervisedLearnerUtils.checkConfig(config));
    }

    private void myCheck(final String params, final String aParam, final String aPrefix) {
        myCheck(params, aPrefix + " parameter '" + aParam + "' missing.");
    }

    private void myCheck(final String params, final String aMsg) {
        Config config = ConfigFactory.parseString(params);
        assertEquals("Configuration should be recognized as invalid", aMsg, SupervisedLearnerUtils.checkConfig(config));
    }

}