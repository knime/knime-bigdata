package com.knime.bigdata.spark.jobserver.jobs;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.knime.base.node.preproc.joiner.Joiner2Settings.JoinMode;

import com.knime.bigdata.spark.jobserver.jobs.JoinJob;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.node.preproc.joiner.SparkJoinerTask;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class JoinJobParametersTest {

    private static String getParams(final String aLeftTab, final String aRightTab,
        final org.knime.base.node.preproc.joiner.Joiner2Settings.JoinMode aJoinMode, final Integer[] aJoinColIdxesLeft,
        final Integer[] aJoinColIdxesRight, final Integer[] aSelectColIdxesLeft, final Integer[] aSelectColIdxesRight,
        final String aOutputDataPath1) {

        return SparkJoinerTask.joinParams(aLeftTab, aRightTab, aJoinMode, aJoinColIdxesLeft, aJoinColIdxesRight,
            aSelectColIdxesLeft, aSelectColIdxesRight, aOutputDataPath1);
    }

    @Test
    public void jobValidationShouldCheckMissingInputTable1Parameter() throws Throwable {
        String params =
            getParams(null, "tab2", JoinMode.InnerJoin, new Integer[]{1, 5, 2, 7}, new Integer[]{1, 5, 2, 7},
                new Integer[]{1, 5, 2, 7}, new Integer[]{1, 5, 2, 7}, "OutTab");
        myCheck(params, ParameterConstants.PARAM_TABLE_1, "Input");
    }

    @Test
    public void jobValidationShouldCheckMissingInputTable2Parameter() throws Throwable {
        String params =
            getParams("tab1", null, JoinMode.InnerJoin, new Integer[]{1, 5, 2, 7}, new Integer[]{1, 5, 2, 7},
                new Integer[]{1, 5, 2, 7}, new Integer[]{1, 5, 2, 7}, "OutTab");
        myCheck(params, ParameterConstants.PARAM_TABLE_2, "Input");
    }

    @Test(expected=NullPointerException.class)
    public void jobValidationShouldCheckMissingJoinModeParameter() throws Throwable {
       getParams("tab1", "tab2", null, new Integer[]{1, 5, 2, 7}, new Integer[]{1, 5, 2, 7}, new Integer[]{1, 5,
                2, 7}, new Integer[]{1, 5, 2, 7}, "OutTab");
        //myCheck(params, ParameterConstants.PARAM_STRING, "Input");
    }

    @Test
    public void jobValidationShouldCheckMissingLeftColSelectionParameter() throws Throwable {
        String params =
            getParams("tab1", "tab2", JoinMode.InnerJoin, new Integer[]{1, 5, 2, 7}, new Integer[]{1, 5, 2, 7}, null,
                new Integer[]{1, 5, 2, 7}, "OutTab");
        myCheck(params, "Input parameter '" + ParameterConstants.NUMBERED_PARAM(ParameterConstants.PARAM_COL_IDXS, 2)
            + "' is not of expected type 'integer list'.");
    }

    @Test
    public void jobValidationShouldCheckMissingRightColSelectionParameter() throws Throwable {
        String params =
            getParams("tab1", "tab2", JoinMode.FullOuterJoin, new Integer[]{1, 5, 2, 7}, new Integer[]{1, 5, 2, 7},
                new Integer[]{1, 5, 2, 7}, null, "OutTab");
        myCheck(params, "Input parameter '" + ParameterConstants.NUMBERED_PARAM(ParameterConstants.PARAM_COL_IDXS, 3)
            + "' is not of expected type 'integer list'.");
    }

    @Test
    public void jobValidationShouldCheckMissingLeftColJoinParameter() throws Throwable {
        String params =
            getParams("tab1", "tab2", JoinMode.LeftOuterJoin, null, new Integer[]{1, 5, 2, 7},
                new Integer[]{1, 5, 2, 7}, new Integer[]{1, 5, 2, 7}, "OutTab");
        myCheck(params, "Input parameter '" + ParameterConstants.NUMBERED_PARAM(ParameterConstants.PARAM_COL_IDXS, 0)
            + "' is not of expected type 'integer list'.");
    }

    @Test
    public void jobValidationShouldCheckMissingRightColJoinParameter() throws Throwable {
        String params =
            getParams("tab1", "tab2", JoinMode.RightOuterJoin, new Integer[]{1, 5, 2, 7}, null, new Integer[]{1, 5, 2,
                7}, new Integer[]{1, 5, 2, 7}, "OutTab");
        myCheck(params, "Input parameter '" + ParameterConstants.NUMBERED_PARAM(ParameterConstants.PARAM_COL_IDXS, 1)
            + "' is not of expected type 'integer list'.");
    }

    @Test
    public void jobValidationShouldCheckIncorrectColSelectionParameter() throws Throwable {
        String params =
            getParams("tab1", "tab2", JoinMode.InnerJoin, new Integer[]{1, 5, 2, 7}, new Integer[]{1, 5, 2, 7},
                new Integer[]{}, new Integer[]{1, 5, 2, 7}, "OutTab");
        String msg =
            "Input parameter '" + ParameterConstants.NUMBERED_PARAM(ParameterConstants.PARAM_COL_IDXS, 2)
                + "' is empty.";
        myCheck(params, msg);

    }

    @Test
    public void jobValidationShouldCheckMissingOuputParameter1() throws Throwable {
        String params =
            getParams("tab1", "tab2", JoinMode.LeftOuterJoin, new Integer[]{1, 5, 2, 7}, new Integer[]{1, 5, 2, 7},
                new Integer[]{1, 5, 2, 7}, new Integer[]{1, 5, 2, 7}, null);
        myCheck(params, ParameterConstants.PARAM_TABLE_1, "Output");

    }

    private void myCheck(final String params, final String aParam, final String aPrefix) {
        myCheck(params, aPrefix + " parameter '" + aParam + "' missing.");
    }

    private void myCheck(final String params, final String aMsg) {
        KnimeSparkJob testObj = new JoinJob();
        JobConfig config = new JobConfig(ConfigFactory.parseString(params));
        assertEquals("Configuration should be recognized as invalid", ValidationResultConverter.invalid(aMsg),
            testObj.validate(config));
    }

    @Test
    public void verifyThatJoinModeEnumsAreIdentical() {
        org.knime.base.node.preproc.joiner.Joiner2Settings.JoinMode[] values =
            org.knime.base.node.preproc.joiner.Joiner2Settings.JoinMode.values();
        for (org.knime.base.node.preproc.joiner.Joiner2Settings.JoinMode value : values) {
            assertEquals("enums must be identical", value.toString(), com.knime.bigdata.spark.jobserver.server.JoinMode
                .fromKnimeJoinMode(value.toString()).toString());
        }

    }
}