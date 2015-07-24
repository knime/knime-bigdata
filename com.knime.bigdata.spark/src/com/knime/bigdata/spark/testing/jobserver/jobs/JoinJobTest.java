package com.knime.bigdata.spark.testing.jobserver.jobs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JobStatus;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.client.KnimeContext;
import com.knime.bigdata.spark.jobserver.jobs.FetchRowsJob;
import com.knime.bigdata.spark.jobserver.jobs.JoinJob;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.JoinMode;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.testing.SparkSpec;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class JoinJobTest extends SparkSpec {

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
    public void runningJoinJobDirectlyShouldProduceResult() throws Throwable {
        KNIMESparkContext contextName = KnimeContext.getSparkContext();

        ImportKNIMETableJobTest.importTestTable(contextName, "tab1");
        ImportKNIMETableJobTest.importTestTable(contextName, "tab2");

        final String resTableName = "OutTab";

        String params =
            getParams("tab1", "tab2", JoinMode.InnerJoin.toString(), new Integer[]{1,3}, new Integer[]{1, 3},
                new Integer[]{0, 1, 2, 3}, new Integer[]{0, 1, 2, 3}, resTableName);

        String jobId = JobControler.startJob(contextName, JoinJob.class.getCanonicalName(), params.toString());

        JobControler.waitForJobAndFetchResult(contextName, jobId, null);
        checkResult(contextName, resTableName);
    }

    private void checkResult(final KNIMESparkContext aContextName, final String resTableName) throws Exception {

        // now check result:
        String takeJobId =
            JobControler.startJob(aContextName, FetchRowsJob.class.getCanonicalName(), rowFetcherDef(10, resTableName));
        assertFalse("job should have finished properly",
            JobControler.waitForJob(aContextName, takeJobId, null).equals(JobStatus.UNKNOWN));
        JobResult res = JobControler.fetchJobResult(aContextName, takeJobId);
        assertNotNull("row fetcher must return a result", res);

        Object[][] arrayRes = (Object[][])res.getObjectResult();
        assertEquals("fetcher should return correct number of rows", 4, arrayRes.length);
        assertArrayEquals(ImportKNIMETableJobTest.TEST_TABLE[0], arrayRes[0]);
        assertArrayEquals(ImportKNIMETableJobTest.TEST_TABLE[1], arrayRes[1]);
        assertArrayEquals(ImportKNIMETableJobTest.TEST_TABLE[2], arrayRes[2]);
        assertArrayEquals(ImportKNIMETableJobTest.TEST_TABLE[3], arrayRes[3]);
    }


    private String rowFetcherDef(final int aNumRows, final String aTableName) {
        return JsonUtils.asJson(new Object[]{
            ParameterConstants.PARAM_INPUT,
            new String[]{ParameterConstants.PARAM_NUMBER_ROWS, "" + aNumRows, ParameterConstants.PARAM_TABLE_1,
                aTableName}});
    }

}