package com.knime.bigdata.spark.testing.jobserver.jobs;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.jobs.JoinJob;
import com.knime.bigdata.spark.jobserver.server.JoinMode;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class JoinJobParametersTest {

    //  //Indices der Joinspalten des linken RDDS
    //    final int[] leftJoinColumns;
    //    //Indices der Joinspalten des rechten RDDS
    //    final int[] rightJoinColumns;
    //    //Indices der Spalten die vom linken RDD übernommen werden sollen inkl.
    //    // Joinspalten wenn gewünscht
    //    final Integer[] leftIncludCols;
    //    //Indices der Spalten die vom rechten RDD übernommen werden sollen inkl.
    //    //Joinspalten wenn gewünscht
    //    final Integer[] rightIncludCols;
    //    //Modus: Inner, left/right/full outer join
    //    final JoinMode joinMode;

    static String getInputOutputParamPair(final String aLeftTab, final String aRightTab, final String aJoinMode,
        final Integer[] aJoinColIdxesLeft, final Integer[] aJoinColIdxesRight, final Integer[] aSelectColIdxesLeft,
        final Integer[] aSelectColIdxesRight, final String aOutputDataPath1) {
        StringBuilder params = new StringBuilder("");
        params.append("   \"").append(ParameterConstants.PARAM_INPUT).append("\" {\n");

        if (aLeftTab != null) {
            params.append("         \"").append(ParameterConstants.PARAM_TABLE_1).append("\": \"").append(aLeftTab)
                .append("\",\n");
        }
        if (aRightTab != null) {
            params.append("         \"").append(ParameterConstants.PARAM_TABLE_2).append("\": \"").append(aRightTab)
                .append("\",\n");
        }

        if (aJoinMode != null) {
            params.append("         \"").append(ParameterConstants.PARAM_STRING).append("\": \"")
                .append(aJoinMode.toString()).append("\",\n");
        }

        if (aJoinColIdxesLeft != null) {
            params.append("         \"")
                .append(ParameterConstants.NUMBERED_PARAM(ParameterConstants.PARAM_COL_IDXS, 0)).append("\": ")
                .append(JsonUtils.toJsonArray(aJoinColIdxesLeft)).append(",\n");
        }
        if (aJoinColIdxesRight != null) {
            params.append("         \"")
                .append(ParameterConstants.NUMBERED_PARAM(ParameterConstants.PARAM_COL_IDXS, 1)).append("\": ")
                .append(JsonUtils.toJsonArray(aJoinColIdxesRight)).append(",\n");
        }
        if (aSelectColIdxesLeft != null) {
            params.append("         \"")
                .append(ParameterConstants.NUMBERED_PARAM(ParameterConstants.PARAM_COL_IDXS, 2)).append("\": ")
                .append(JsonUtils.toJsonArray(aSelectColIdxesLeft)).append(",\n");
        }
        if (aSelectColIdxesRight != null) {
            params.append("         \"")
                .append(ParameterConstants.NUMBERED_PARAM(ParameterConstants.PARAM_COL_IDXS, 3)).append("\": ")
                .append(JsonUtils.toJsonArray(aSelectColIdxesRight)).append(",\n");
        }

        params.append("    }\n");
        params.append("    \"").append(ParameterConstants.PARAM_OUTPUT).append("\" {\n");
        if (aOutputDataPath1 != null) {
            params.append("         \"").append(ParameterConstants.PARAM_TABLE_1).append("\": \"")
                .append(aOutputDataPath1).append("\"\n");
        }
        params.append("    }\n");
        params.append("    \n");
        return params.toString();
    }

    private static String getParams(final String aLeftTab, final String aRightTab, final String aJoinMode,
        final Integer[] aJoinColIdxesLeft, final Integer[] aJoinColIdxesRight, final Integer[] aSelectColIdxesLeft,
        final Integer[] aSelectColIdxesRight, final String aOutputDataPath1) {
        StringBuilder params = new StringBuilder("{\n");
        params.append(getInputOutputParamPair(aLeftTab, aRightTab, aJoinMode, aJoinColIdxesLeft, aJoinColIdxesRight,
            aSelectColIdxesLeft, aSelectColIdxesRight, aOutputDataPath1));
        params.append("}");
        return params.toString();
    }

    @Test
    public void jobValidationShouldCheckMissingInputTable1Parameter() throws Throwable {
        String params =
            getParams(null, "tab2", JoinMode.InnerJoin.toString(), new Integer[]{1, 5, 2, 7},
                new Integer[]{1, 5, 2, 7}, new Integer[]{1, 5, 2, 7}, new Integer[]{1, 5, 2, 7}, "OutTab");
        myCheck(params, ParameterConstants.PARAM_INPUT + "." + ParameterConstants.PARAM_TABLE_1, "Input");
    }

    @Test
    public void jobValidationShouldCheckMissingInputTable2Parameter() throws Throwable {
        String params =
            getParams("tab1", null, JoinMode.InnerJoin.toString(), new Integer[]{1, 5, 2, 7},
                new Integer[]{1, 5, 2, 7}, new Integer[]{1, 5, 2, 7}, new Integer[]{1, 5, 2, 7}, "OutTab");
        myCheck(params, ParameterConstants.PARAM_INPUT + "." + ParameterConstants.PARAM_TABLE_2, "Input");
    }

    @Test
    public void jobValidationShouldCheckMissingJoinModeParameter() throws Throwable {
        String params =
            getParams("tab1", "tab2", null, new Integer[]{1, 5, 2, 7}, new Integer[]{1, 5, 2, 7}, new Integer[]{1, 5,
                2, 7}, new Integer[]{1, 5, 2, 7}, "OutTab");
        myCheck(params, ParameterConstants.PARAM_INPUT + "." + ParameterConstants.PARAM_STRING, "Input");
    }

    @Test
    public void jobValidationShouldCheckIncorrectJoinModeParameter() throws Throwable {
        String params =
            getParams("tab1", "tab2", "MyJoin", new Integer[]{1, 5, 2, 7}, new Integer[]{1, 5, 2, 7}, new Integer[]{1,
                5, 2, 7}, new Integer[]{1, 5, 2, 7}, "OutTab");
        String msg =
            "Input parameter '" + ParameterConstants.PARAM_INPUT + "." + ParameterConstants.PARAM_STRING
                + "' has an invalid value.";
        myCheck(params, msg);
    }

    @Test
    public void jobValidationShouldCheckMissingLeftColSelectionParameter() throws Throwable {
        String params =
            getParams("tab1", "tab2", JoinMode.InnerJoin.toString(), new Integer[]{1, 5, 2, 7}, new Integer[]{1, 5, 2,
                7}, null, new Integer[]{1, 5, 2, 7}, "OutTab");
        myCheck(
            params,
            ParameterConstants.PARAM_INPUT + "."
                + ParameterConstants.NUMBERED_PARAM(ParameterConstants.PARAM_COL_IDXS, 2), "Input");
    }

    @Test
    public void jobValidationShouldCheckMissingRightColSelectionParameter() throws Throwable {
        String params =
            getParams("tab1", "tab2", JoinMode.InnerJoin.toString(), new Integer[]{1, 5, 2, 7}, new Integer[]{1, 5, 2,
                7}, new Integer[]{1, 5, 2, 7}, null, "OutTab");
        myCheck(
            params,
            ParameterConstants.PARAM_INPUT + "."
                + ParameterConstants.NUMBERED_PARAM(ParameterConstants.PARAM_COL_IDXS, 3), "Input");
    }

    @Test
    public void jobValidationShouldCheckMissingLeftColJoinParameter() throws Throwable {
        String params =
            getParams("tab1", "tab2", JoinMode.InnerJoin.toString(), null, new Integer[]{1, 5, 2, 7}, new Integer[]{1,
                5, 2, 7}, new Integer[]{1, 5, 2, 7}, "OutTab");
        myCheck(
            params,
            ParameterConstants.PARAM_INPUT + "."
                + ParameterConstants.NUMBERED_PARAM(ParameterConstants.PARAM_COL_IDXS, 0), "Input");
    }

    @Test
    public void jobValidationShouldCheckMissingRightColJoinParameter() throws Throwable {
        String params =
            getParams("tab1", "tab2", JoinMode.InnerJoin.toString(), new Integer[]{1, 5, 2, 7}, null, new Integer[]{1,
                5, 2, 7}, new Integer[]{1, 5, 2, 7}, "OutTab");
        myCheck(
            params,
            ParameterConstants.PARAM_INPUT + "."
                + ParameterConstants.NUMBERED_PARAM(ParameterConstants.PARAM_COL_IDXS, 1), "Input");
    }

    @Test
    public void jobValidationShouldCheckIncorrectColSelectionParameter() throws Throwable {
        String params =
            getParams("tab1", "tab2", JoinMode.InnerJoin.toString(), new Integer[]{1, 5, 2, 7}, new Integer[]{1, 5, 2,
                7}, new Integer[]{}, new Integer[]{1, 5, 2, 7}, "OutTab");
        String msg =
            "Input parameter '" + ParameterConstants.PARAM_INPUT + "."
                + ParameterConstants.NUMBERED_PARAM(ParameterConstants.PARAM_COL_IDXS, 2) + "' is empty.";
        myCheck(params, msg);

    }

    @Test
    public void jobValidationShouldCheckMissingOuputParameter1() throws Throwable {
        String params =
            getParams("tab1", "tab2", JoinMode.InnerJoin.toString(), new Integer[]{1, 5, 2, 7}, new Integer[]{1, 5, 2,
                7}, new Integer[]{1, 5, 2, 7}, new Integer[]{1, 5, 2, 7}, null);
        myCheck(params, ParameterConstants.PARAM_OUTPUT + "." + ParameterConstants.PARAM_TABLE_1, "Output");

    }

    private void myCheck(final String params, final String aParam, final String aPrefix) {
        myCheck(params, aPrefix + " parameter '" + aParam + "' missing.");
    }

    private void myCheck(final String params, final String aMsg) {
        KnimeSparkJob testObj = new JoinJob();
        Config config = ConfigFactory.parseString(params);
        assertEquals("Configuration should be recognized as invalid", ValidationResultConverter.invalid(aMsg),
            testObj.validate(config));
    }

    @Test
    public void verifyThatJoinModeEnumsAreIdentical() {
       org.knime.base.node.preproc.joiner.Joiner2Settings.JoinMode[] values = org.knime.base.node.preproc.joiner.Joiner2Settings.JoinMode.values();
       for (org.knime.base.node.preproc.joiner.Joiner2Settings.JoinMode value : values) {
           assertEquals("enums must be identical", value.toString(), JoinMode.fromKnimeJoinMode(value.toString()).toString());
       }

    }
}