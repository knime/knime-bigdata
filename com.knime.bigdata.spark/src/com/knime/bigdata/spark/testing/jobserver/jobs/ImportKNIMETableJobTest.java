package com.knime.bigdata.spark.testing.jobserver.jobs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import org.knime.core.node.CanceledExecutionException;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JobStatus;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.client.KnimeContext;
import com.knime.bigdata.spark.jobserver.jobs.FetchRowsJob;
import com.knime.bigdata.spark.jobserver.jobs.ImportKNIMETableJob;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.node.io.table.writer.Table2SparkNodeModel;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.testing.SparkSpec;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class ImportKNIMETableJobTest extends SparkSpec {

    static final Object[][] TEST_TABLE = new Object[][]{new Object[]{1, true, 3.2d, "my string"},
                new Object[]{2, false, 3.2d, "my string"},
                new Object[]{3, true, 38d, "my other string"},
                new Object[]{4, false, 34.2d, "my other string"}};

    @Test
    public void runningImportJobDirectlyShouldProduceResult() throws Throwable {
        KNIMESparkContext contextName = KnimeContext.getSparkContext();

        final String resTableName = "knimeTab1";
        String jobId = importTestTable(contextName, TEST_TABLE, new Class<?>[]{Integer.class,
            Boolean.class, Double.class, String.class}, resTableName);


        // result is serialized as a string
        assertFalse("job should not be running anymore",
            JobStatus.OK.equals(JobControler.getJobStatus(contextName, jobId)));

        checkResult(contextName, resTableName);
    }

    /**
     * @param contextName
     * @param resTableName
     * @return
     * @throws GenericKnimeSparkException
     * @throws CanceledExecutionException
     */
    static String importTestTable(final KNIMESparkContext contextName, final Object[][] aTable, final Class<?>[] aTypes, final String resTableName)
        throws GenericKnimeSparkException, CanceledExecutionException {
        String params =
                Table2SparkNodeModel.paramDef(aTable, aTypes, resTableName);
        String jobId = JobControler.startJob(contextName, ImportKNIMETableJob.class.getCanonicalName(), params.toString());

        JobControler.waitForJobAndFetchResult(contextName, jobId, null);
        return jobId;
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
        assertArrayEquals(TEST_TABLE[0], arrayRes[0]);
        assertArrayEquals(TEST_TABLE[1], arrayRes[1]);
        assertArrayEquals(TEST_TABLE[2], arrayRes[2]);
        assertArrayEquals(TEST_TABLE[3], arrayRes[3]);
    }

    private String rowFetcherDef(final int aNumRows, final String aTableName) {
        return JsonUtils.asJson(new Object[]{
            ParameterConstants.PARAM_INPUT,
            new String[]{ParameterConstants.PARAM_NUMBER_ROWS, "" + aNumRows, ParameterConstants.PARAM_TABLE_1,
                aTableName}});
    }

}