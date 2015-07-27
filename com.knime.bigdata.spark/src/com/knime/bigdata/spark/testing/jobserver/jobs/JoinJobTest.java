package com.knime.bigdata.spark.testing.jobserver.jobs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Test;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JobStatus;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.client.KnimeContext;
import com.knime.bigdata.spark.jobserver.jobs.FetchRowsJob;
import com.knime.bigdata.spark.jobserver.jobs.JoinJob;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.JoinMode;
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
                .append(JsonUtils.toJsonArray((Object[])aJoinColIdxesLeft)).append(",\n");
        }
        if (aJoinColIdxesRight != null) {
            params.append("         \"")
                .append(ParameterConstants.NUMBERED_PARAM(ParameterConstants.PARAM_COL_IDXS, 1)).append("\": ")
                .append(JsonUtils.toJsonArray((Object[])aJoinColIdxesRight)).append(",\n");
        }
        if (aSelectColIdxesLeft != null) {
            params.append("         \"")
                .append(ParameterConstants.NUMBERED_PARAM(ParameterConstants.PARAM_COL_IDXS, 2)).append("\": ")
                .append(JsonUtils.toJsonArray((Object[])aSelectColIdxesLeft)).append(",\n");
        }
        if (aSelectColIdxesRight != null) {
            params.append("         \"")
                .append(ParameterConstants.NUMBERED_PARAM(ParameterConstants.PARAM_COL_IDXS, 3)).append("\": ")
                .append(JsonUtils.toJsonArray((Object[])aSelectColIdxesRight)).append(",\n");
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
    public void innerJoinOfIdenticalTableWithOneMatchingColumn() throws Throwable {
        KNIMESparkContext contextName = KnimeContext.getSparkContext();

        ImportKNIMETableJobTest.importTestTable(contextName, ImportKNIMETableJobTest.TEST_TABLE, new Class<?>[]{
            Integer.class, Boolean.class, Double.class, String.class}, "tab1");
        ImportKNIMETableJobTest.importTestTable(contextName, ImportKNIMETableJobTest.TEST_TABLE, new Class<?>[]{
            Integer.class, Boolean.class, Double.class, String.class}, "tab2");

        final String resTableName = "OutTab";

        String params =
            getParams("tab1", "tab2", JoinMode.InnerJoin.toString(), new Integer[]{1, 3}, new Integer[]{1, 3},
                new Integer[]{0, 1, 2, 3}, new Integer[]{0, 1, 2, 3}, resTableName);

        String jobId = JobControler.startJob(contextName, JoinJob.class.getCanonicalName(), params.toString());

        Object[][] expected = new Object[4][];
        for (int i = 0; i < expected.length; i++) {
            expected[i] =
                new Object[]{ImportKNIMETableJobTest.TEST_TABLE[i][0], ImportKNIMETableJobTest.TEST_TABLE[i][1],
                    ImportKNIMETableJobTest.TEST_TABLE[i][2], ImportKNIMETableJobTest.TEST_TABLE[i][3],
                    ImportKNIMETableJobTest.TEST_TABLE[i][0], ImportKNIMETableJobTest.TEST_TABLE[i][1],
                    ImportKNIMETableJobTest.TEST_TABLE[i][2], ImportKNIMETableJobTest.TEST_TABLE[i][3]};
        }
        JobControler.waitForJobAndFetchResult(contextName, jobId, null);
        checkResult(contextName, resTableName, expected);
    }

    @Test
    public void innerJoinOfTwoTablesWithTwoMatchingColumns() throws Throwable {
        KNIMESparkContext contextName = KnimeContext.getSparkContext();

        ImportKNIMETableJobTest.importTestTable(contextName, ImportKNIMETableJobTest.TEST_TABLE, new Class<?>[]{
            Integer.class, Boolean.class, Double.class, String.class}, "tab1");
        ImportKNIMETableJobTest.importTestTable(contextName, TEST_TABLE_2, new Class<?>[]{Integer.class, String.class,
            String.class}, "tab2");

        final String resTableName = "OutTab";

        String params =
            getParams("tab1", "tab2", JoinMode.InnerJoin.toString(), new Integer[]{0, 3}, new Integer[]{0, 2},
                new Integer[]{0, 1, 2}, new Integer[]{2, 1}, resTableName);

        Config config = ConfigFactory.parseString(params);
        assertEquals("Configuration should be recognized as valid", ValidationResultConverter.valid(),
            new JoinJob().validate(config));

        String jobId = JobControler.startJob(contextName, JoinJob.class.getCanonicalName(), params.toString());

        Object[][] expected = new Object[2][];

        expected[0] =
            new Object[]{ImportKNIMETableJobTest.TEST_TABLE[0][0], ImportKNIMETableJobTest.TEST_TABLE[0][1],
                ImportKNIMETableJobTest.TEST_TABLE[0][2], TEST_TABLE_2[0][2], TEST_TABLE_2[0][1]};
        expected[1] =
            new Object[]{ImportKNIMETableJobTest.TEST_TABLE[0][0], ImportKNIMETableJobTest.TEST_TABLE[0][1],
                ImportKNIMETableJobTest.TEST_TABLE[0][2], TEST_TABLE_2[1][2], TEST_TABLE_2[1][1]};

        JobControler.waitForJobAndFetchResult(contextName, jobId, null);
        checkResult(contextName, resTableName, expected);
    }

    @Test
    public void innerJoinOfTwoTablesWithMultipleMatches() throws Throwable {
        KNIMESparkContext contextName = KnimeContext.getSparkContext();

        ImportKNIMETableJobTest.importTestTable(contextName, TEST_TABLE_3, new Class<?>[]{Integer.class, String.class,
            String.class}, "tab1");
        ImportKNIMETableJobTest.importTestTable(contextName, TEST_TABLE_2, new Class<?>[]{Integer.class, String.class,
            String.class}, "tab2");

        final String resTableName = "OutTab";

        String params =
            getParams("tab1", "tab2", JoinMode.InnerJoin.toString(), new Integer[]{0, 2}, new Integer[]{0, 2},
                new Integer[]{0, 1, 2}, new Integer[]{2, 1}, resTableName);

        Config config = ConfigFactory.parseString(params);
        assertEquals("Configuration should be recognized as valid", ValidationResultConverter.valid(),
            new JoinJob().validate(config));

        String jobId = JobControler.startJob(contextName, JoinJob.class.getCanonicalName(), params.toString());

        Object[][] expected = new Object[6][];

        expected[0] =
            new Object[]{TEST_TABLE_3[0][0], TEST_TABLE_3[0][1], TEST_TABLE_3[0][2], TEST_TABLE_2[0][2],
                TEST_TABLE_2[0][1]};
        expected[1] =
            new Object[]{TEST_TABLE_3[0][0], TEST_TABLE_3[0][1], TEST_TABLE_3[0][2], TEST_TABLE_2[1][2],
                TEST_TABLE_2[1][1]};
        expected[2] =
            new Object[]{TEST_TABLE_3[1][0], TEST_TABLE_3[1][1], TEST_TABLE_3[1][2], TEST_TABLE_2[0][2],
                TEST_TABLE_2[0][1]};
        expected[3] =
            new Object[]{TEST_TABLE_3[1][0], TEST_TABLE_3[1][1], TEST_TABLE_3[1][2], TEST_TABLE_2[1][2],
                TEST_TABLE_2[1][1]};
        expected[4] =
            new Object[]{TEST_TABLE_3[3][0], TEST_TABLE_3[3][1], TEST_TABLE_3[3][2], TEST_TABLE_2[2][2],
                TEST_TABLE_2[2][1]};
        expected[5] =
            new Object[]{TEST_TABLE_3[3][0], TEST_TABLE_3[3][1], TEST_TABLE_3[3][2], TEST_TABLE_2[3][2],
                TEST_TABLE_2[3][1]};
        JobControler.waitForJobAndFetchResult(contextName, jobId, null);
        checkResult(contextName, resTableName, expected);
    }

    @Test
    public void leftOuterJoinOfTwoTablesWithMultipleMatches() throws Throwable {
        KNIMESparkContext contextName = KnimeContext.getSparkContext();

        ImportKNIMETableJobTest.importTestTable(contextName, TEST_TABLE_3, new Class<?>[]{Integer.class, String.class,
            String.class}, "tab1");
        ImportKNIMETableJobTest.importTestTable(contextName, TEST_TABLE_2, new Class<?>[]{Integer.class, String.class,
            String.class}, "tab2");

        final String resTableName = "OutTab";

        String params =
            getParams("tab1", "tab2", JoinMode.LeftOuterJoin.toString(), new Integer[]{0, 2}, new Integer[]{0, 2},
                new Integer[]{0, 1, 2}, new Integer[]{2, 1}, resTableName);

        Config config = ConfigFactory.parseString(params);
        assertEquals("Configuration should be recognized as valid", ValidationResultConverter.valid(),
            new JoinJob().validate(config));

        String jobId = JobControler.startJob(contextName, JoinJob.class.getCanonicalName(), params.toString());

        Object[][] expected = new Object[7][];

        expected[0] =
            new Object[]{TEST_TABLE_3[0][0], TEST_TABLE_3[0][1], TEST_TABLE_3[0][2], TEST_TABLE_2[0][2],
                TEST_TABLE_2[0][1]};
        expected[1] =
            new Object[]{TEST_TABLE_3[0][0], TEST_TABLE_3[0][1], TEST_TABLE_3[0][2], TEST_TABLE_2[1][2],
                TEST_TABLE_2[1][1]};
        expected[2] =
            new Object[]{TEST_TABLE_3[1][0], TEST_TABLE_3[1][1], TEST_TABLE_3[1][2], TEST_TABLE_2[0][2],
                TEST_TABLE_2[0][1]};
        expected[3] =
            new Object[]{TEST_TABLE_3[1][0], TEST_TABLE_3[1][1], TEST_TABLE_3[1][2], TEST_TABLE_2[1][2],
                TEST_TABLE_2[1][1]};
        expected[4] =
            new Object[]{TEST_TABLE_3[3][0], TEST_TABLE_3[3][1], TEST_TABLE_3[3][2], TEST_TABLE_2[2][2],
                TEST_TABLE_2[2][1]};
        expected[5] =
            new Object[]{TEST_TABLE_3[3][0], TEST_TABLE_3[3][1], TEST_TABLE_3[3][2], TEST_TABLE_2[3][2],
                TEST_TABLE_2[3][1]};
        expected[6] = new Object[]{TEST_TABLE_3[2][0], TEST_TABLE_3[2][1], TEST_TABLE_3[2][2], null, null};
        JobControler.waitForJobAndFetchResult(contextName, jobId, null);
        checkResult(contextName, resTableName, expected);
    }

    @Test
    public void rightOuterJoinOfTwoTablesWithMultipleMatches() throws Throwable {
        KNIMESparkContext contextName = KnimeContext.getSparkContext();

        ImportKNIMETableJobTest.importTestTable(contextName, TEST_TABLE_2, new Class<?>[]{Integer.class, String.class,
            String.class}, "tab1");
        ImportKNIMETableJobTest.importTestTable(contextName, TEST_TABLE_3, new Class<?>[]{Integer.class, String.class,
            String.class}, "tab2");

        final String resTableName = "OutTab";

        String params =
            getParams("tab1", "tab2", JoinMode.RightOuterJoin.toString(), new Integer[]{0, 2}, new Integer[]{0, 2},
                new Integer[]{0, 1, 2}, new Integer[]{2, 1}, resTableName);

        Config config = ConfigFactory.parseString(params);
        assertEquals("Configuration should be recognized as valid", ValidationResultConverter.valid(),
            new JoinJob().validate(config));

        String jobId = JobControler.startJob(contextName, JoinJob.class.getCanonicalName(), params.toString());

        Object[][] expected = new Object[7][];

        expected[0] =
            new Object[]{TEST_TABLE_2[0][0], TEST_TABLE_2[0][1], TEST_TABLE_2[0][2], TEST_TABLE_3[0][2],
                TEST_TABLE_3[0][1]};
        expected[1] =
            new Object[]{TEST_TABLE_2[0][0], TEST_TABLE_2[0][1], TEST_TABLE_2[0][2], TEST_TABLE_3[1][2],
                TEST_TABLE_3[1][1]};
        expected[2] =
            new Object[]{TEST_TABLE_2[1][0], TEST_TABLE_2[1][1], TEST_TABLE_2[1][2], TEST_TABLE_3[0][2],
                TEST_TABLE_3[0][1]};
        expected[3] =
            new Object[]{TEST_TABLE_2[1][0], TEST_TABLE_2[1][1], TEST_TABLE_2[1][2], TEST_TABLE_3[1][2],
                TEST_TABLE_3[1][1]};
        expected[4] =
            new Object[]{TEST_TABLE_2[2][0], TEST_TABLE_2[2][1], TEST_TABLE_2[2][2], TEST_TABLE_3[3][2],
                TEST_TABLE_3[3][1]};
        expected[5] =
            new Object[]{TEST_TABLE_2[3][0], TEST_TABLE_2[3][1], TEST_TABLE_2[3][2], TEST_TABLE_3[3][2],
                TEST_TABLE_3[3][1]};
        expected[6] = new Object[]{null, null, null, TEST_TABLE_3[2][2], TEST_TABLE_3[2][1]};
        JobControler.waitForJobAndFetchResult(contextName, jobId, null);
        checkResult(contextName, resTableName, expected);
    }

    @Test
    public void fullOuterJoinOfTwoTablesWithMultipleMatches() throws Throwable {
        KNIMESparkContext contextName = KnimeContext.getSparkContext();

        ImportKNIMETableJobTest.importTestTable(contextName, TEST_TABLE_2, new Class<?>[]{Integer.class, String.class,
            String.class}, "tab1");
        ImportKNIMETableJobTest.importTestTable(contextName, TEST_TABLE_3, new Class<?>[]{Integer.class, String.class,
            String.class}, "tab2");

        final String resTableName = "OutTab";

        String params =
            getParams("tab1", "tab2", JoinMode.FullOuterJoin.toString(), new Integer[]{1}, new Integer[]{1},
                new Integer[]{0, 1, 2}, new Integer[]{2, 1}, resTableName);

        Config config = ConfigFactory.parseString(params);
        assertEquals("Configuration should be recognized as valid", ValidationResultConverter.valid(),
            new JoinJob().validate(config));

        String jobId = JobControler.startJob(contextName, JoinJob.class.getCanonicalName(), params.toString());

        Object[][] expected = new Object[6][];

        expected[0] =
            new Object[]{TEST_TABLE_2[0][0], TEST_TABLE_2[0][1], TEST_TABLE_2[0][2], TEST_TABLE_3[0][2],
                TEST_TABLE_3[0][1]};
        expected[1] =
            new Object[]{TEST_TABLE_2[1][0], TEST_TABLE_2[1][1], TEST_TABLE_2[1][2], TEST_TABLE_3[1][2],
                TEST_TABLE_3[1][1]};
        expected[2] = new Object[]{TEST_TABLE_2[2][0], TEST_TABLE_2[2][1], TEST_TABLE_2[2][2], null, null};
        expected[3] = new Object[]{TEST_TABLE_2[3][0], TEST_TABLE_2[3][1], TEST_TABLE_2[3][2], null, null};
        expected[4] = new Object[]{null, null, null, TEST_TABLE_3[2][2], TEST_TABLE_3[2][1]};
        expected[5] = new Object[]{null, null, null, TEST_TABLE_3[3][2], TEST_TABLE_3[3][1]};

        JobControler.waitForJobAndFetchResult(contextName, jobId, null);
        checkResult(contextName, resTableName, expected);
    }


    @Test
    public void fullOuterJoinOfTwoTablesWithNoMatches() throws Throwable {
        KNIMESparkContext contextName = KnimeContext.getSparkContext();

        ImportKNIMETableJobTest.importTestTable(contextName, TEST_TABLE_3, new Class<?>[]{Integer.class, String.class,
            String.class}, "tab1");
        ImportKNIMETableJobTest.importTestTable(contextName, TEST_TABLE_4, new Class<?>[]{Integer.class}, "tab2");

        final String resTableName = "OutTab";

        String params =
            getParams("tab1", "tab2", JoinMode.FullOuterJoin.toString(), new Integer[]{0}, new Integer[]{0},
                new Integer[]{0, 1, 2}, new Integer[]{0}, resTableName);

        Config config = ConfigFactory.parseString(params);
        assertEquals("Configuration should be recognized as valid", ValidationResultConverter.valid(),
            new JoinJob().validate(config));

        String jobId = JobControler.startJob(contextName, JoinJob.class.getCanonicalName(), params.toString());

        Object[][] expected = new Object[6][];

        expected[0] =
            new Object[]{TEST_TABLE_3[0][0], TEST_TABLE_3[0][1], TEST_TABLE_3[0][2], null};
        expected[1] =
            new Object[]{TEST_TABLE_3[1][0], TEST_TABLE_3[1][1], TEST_TABLE_3[1][2], null};
        expected[2] = new Object[]{TEST_TABLE_3[2][0], TEST_TABLE_3[2][1], TEST_TABLE_3[2][2], null};
        expected[3] = new Object[]{TEST_TABLE_3[3][0], TEST_TABLE_3[3][1], TEST_TABLE_3[3][2], null};
        expected[4] = new Object[]{null, null, null, TEST_TABLE_4[0][0]};
        expected[5] = new Object[]{null, null, null, TEST_TABLE_4[1][0]};

        JobControler.waitForJobAndFetchResult(contextName, jobId, null);
        checkResult(contextName, resTableName, expected);
    }
    private void
        checkResult(final KNIMESparkContext aContextName, final String resTableName, final Object[][] aExpected)
            throws Exception {

        // now check result:
        String takeJobId =
            JobControler.startJob(aContextName, FetchRowsJob.class.getCanonicalName(), rowFetcherDef(10, resTableName));
        assertFalse("job should have finished properly",
            JobControler.waitForJob(aContextName, takeJobId, null).equals(JobStatus.UNKNOWN));
        JobResult res = JobControler.fetchJobResult(aContextName, takeJobId);
        assertNotNull("row fetcher must return a result", res);

        Object[][] arrayRes = (Object[][])res.getObjectResult();
        assertEquals("fetcher should return correct number of rows", aExpected.length, arrayRes.length);
        for (int i = 0; i < arrayRes.length; i++) {
            boolean found = false;
            for (int j = 0; j < aExpected.length; j++) {
                found = found || Arrays.equals(arrayRes[i], aExpected[j]);
            }
            assertTrue("result row[" + i + "]: " + Arrays.toString(arrayRes[i]) + " - not found.", found);
        }
    }

    static final Object[][] TEST_TABLE_2 = new Object[][]{new Object[]{1, "Ping", "my string"},
        new Object[]{1, "Pong", "my string"}, new Object[]{1, "Ping2", "my other string"},
        new Object[]{1, "Pong2", "my other string"}};

    static final Object[][] TEST_TABLE_3 = new Object[][]{new Object[]{1, "Ping", "my string"},
        new Object[]{1, "Pong", "my string"}, new Object[]{2, "Ping2x", "my other string"},
        new Object[]{1, "Pong2a", "my other string"}};

    static final Object[][] TEST_TABLE_4 = new Object[][]{new Object[]{88}, new Object[]{99}};

    private String rowFetcherDef(final int aNumRows, final String aTableName) {
        return JsonUtils.asJson(new Object[]{
            ParameterConstants.PARAM_INPUT,
            new String[]{ParameterConstants.PARAM_NUMBER_ROWS, "" + aNumRows, ParameterConstants.PARAM_TABLE_1,
                aTableName}});
    }

}