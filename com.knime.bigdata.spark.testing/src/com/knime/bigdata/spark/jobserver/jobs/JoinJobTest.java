package com.knime.bigdata.spark.jobserver.jobs;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.knime.base.node.preproc.joiner.Joiner2Settings.JoinMode;

import com.knime.bigdata.spark.SparkWithJobServerSpec;
import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.KnimeContext;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.node.preproc.joiner.SparkJoinerTask;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class JoinJobTest extends SparkWithJobServerSpec {

    private static String getParams(final String aLeftTab, final String aRightTab, final JoinMode aJoinMode,
        final Integer[] aJoinColIdxesLeft, final Integer[] aJoinColIdxesRight, final Integer[] aSelectColIdxesLeft,
        final Integer[] aSelectColIdxesRight, final String aOutputDataPath1) {
        return SparkJoinerTask.joinParams(aLeftTab, aRightTab, aJoinMode, aJoinColIdxesLeft, aJoinColIdxesRight,
            aSelectColIdxesLeft, aSelectColIdxesRight, aOutputDataPath1);
    }

    @Test
    public void innerJoinOfIdenticalTableWithOneMatchingColumn() throws Throwable {
        KNIMESparkContext contextName = KnimeContext.getSparkContext();

        ImportKNIMETableJobTest.importTestTable(contextName, ImportKNIMETableJobTest.TEST_TABLE, "tab1");
        ImportKNIMETableJobTest.importTestTable(contextName, ImportKNIMETableJobTest.TEST_TABLE, "tab2");

        final String resTableName = "OutTab";

        String params =
            getParams("tab1", "tab2", JoinMode.InnerJoin, new Integer[]{1, 3}, new Integer[]{1, 3}, new Integer[]{0, 1,
                2, 3}, new Integer[]{0, 1, 2, 3}, resTableName);

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

        ImportKNIMETableJobTest.importTestTable(contextName, ImportKNIMETableJobTest.TEST_TABLE, "tab1");
        ImportKNIMETableJobTest.importTestTable(contextName, TEST_TABLE_2, "tab2");

        final String resTableName = "OutTab";

        String params =
            getParams("tab1", "tab2", JoinMode.InnerJoin, new Integer[]{0, 3}, new Integer[]{0, 2}, new Integer[]{0, 1,
                2}, new Integer[]{2, 1}, resTableName);

        JobConfig config = new JobConfig(ConfigFactory.parseString(params));
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

        ImportKNIMETableJobTest.importTestTable(contextName, TEST_TABLE_3, "tab1");
        ImportKNIMETableJobTest.importTestTable(contextName, TEST_TABLE_2, "tab2");

        final String resTableName = "OutTab";

        String params =
            getParams("tab1", "tab2", JoinMode.InnerJoin, new Integer[]{0, 2}, new Integer[]{0, 2}, new Integer[]{0, 1,
                2}, new Integer[]{2, 1}, resTableName);

        JobConfig config = new JobConfig(ConfigFactory.parseString(params));
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

        ImportKNIMETableJobTest.importTestTable(contextName, TEST_TABLE_3, "tab1");
        ImportKNIMETableJobTest.importTestTable(contextName, TEST_TABLE_2, "tab2");

        final String resTableName = "OutTab";

        String params =
            getParams("tab1", "tab2", JoinMode.LeftOuterJoin, new Integer[]{0, 2}, new Integer[]{0, 2}, new Integer[]{
                0, 1, 2}, new Integer[]{2, 1}, resTableName);

        JobConfig config = new JobConfig(ConfigFactory.parseString(params));
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

        ImportKNIMETableJobTest.importTestTable(contextName, TEST_TABLE_2, "tab1");
        ImportKNIMETableJobTest.importTestTable(contextName, TEST_TABLE_3, "tab2");

        final String resTableName = "OutTab";

        String params =
            getParams("tab1", "tab2", JoinMode.RightOuterJoin, new Integer[]{0, 2}, new Integer[]{0, 2}, new Integer[]{
                0, 1, 2}, new Integer[]{2, 1}, resTableName);

        JobConfig config = new JobConfig(ConfigFactory.parseString(params));
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

        ImportKNIMETableJobTest.importTestTable(contextName, TEST_TABLE_2, "tab1");
        ImportKNIMETableJobTest.importTestTable(contextName, TEST_TABLE_3, "tab2");

        final String resTableName = "OutTab";

        String params =
            getParams("tab1", "tab2", JoinMode.FullOuterJoin, new Integer[]{1}, new Integer[]{1},
                new Integer[]{0, 1, 2}, new Integer[]{2, 1}, resTableName);

        JobConfig config = new JobConfig(ConfigFactory.parseString(params));
        assertEquals("Configuration should be recognized as valid", ValidationResultConverter.valid(),
            new JoinJob().validate(config));

        String jobId = JobControler.startJob(contextName, JoinJob.class.getCanonicalName(), params);

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

        ImportKNIMETableJobTest.importTestTable(contextName, TEST_TABLE_3, "tab1");
        ImportKNIMETableJobTest.importTestTable(contextName, TEST_TABLE_4, "tab2");

        final String resTableName = "OutTab";

        String params =
            getParams("tab1", "tab2", JoinMode.FullOuterJoin, new Integer[]{0}, new Integer[]{0},
                new Integer[]{0, 1, 2}, new Integer[]{0}, resTableName);

        JobConfig config = new JobConfig(ConfigFactory.parseString(params));
        assertEquals("Configuration should be recognized as valid", ValidationResultConverter.valid(),
            new JoinJob().validate(config));

        String jobId = JobControler.startJob(contextName, JoinJob.class.getCanonicalName(), params.toString());

        Object[][] expected = new Object[6][];

        expected[0] = new Object[]{TEST_TABLE_3[0][0], TEST_TABLE_3[0][1], TEST_TABLE_3[0][2], null};
        expected[1] = new Object[]{TEST_TABLE_3[1][0], TEST_TABLE_3[1][1], TEST_TABLE_3[1][2], null};
        expected[2] = new Object[]{TEST_TABLE_3[2][0], TEST_TABLE_3[2][1], TEST_TABLE_3[2][2], null};
        expected[3] = new Object[]{TEST_TABLE_3[3][0], TEST_TABLE_3[3][1], TEST_TABLE_3[3][2], null};
        expected[4] = new Object[]{null, null, null, TEST_TABLE_4[0][0]};
        expected[5] = new Object[]{null, null, null, TEST_TABLE_4[1][0]};

        JobControler.waitForJobAndFetchResult(contextName, jobId, null);
        checkResult(contextName, resTableName, expected);
    }


    static final Object[][] TEST_TABLE_2 = new Object[][]{new Object[]{1, "Ping", "my string"},
        new Object[]{1, "Pong", "my string"}, new Object[]{1, "Ping2", "my other string"},
        new Object[]{1, "Pong2", "my other string"}};

    static final Object[][] TEST_TABLE_3 = new Object[][]{new Object[]{1, "Ping", "my string"},
        new Object[]{1, "Pong", "my string"}, new Object[]{2, "Ping2x", "my other string"},
        new Object[]{1, "Pong2a", "my other string"}};

    static final Object[][] TEST_TABLE_4 = new Object[][]{new Object[]{88}, new Object[]{99}};


}