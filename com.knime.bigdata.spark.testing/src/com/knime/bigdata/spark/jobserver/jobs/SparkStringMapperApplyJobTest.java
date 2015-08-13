package com.knime.bigdata.spark.jobserver.jobs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.knime.bigdata.spark.SparkWithJobServerSpec;
import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.jobs.ApplyNominalValueMappingJob;
import com.knime.bigdata.spark.jobserver.jobs.ConvertNominalValuesJob;
import com.knime.bigdata.spark.jobserver.jobs.FetchRowsJob;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.MappingType;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.node.preproc.convert.stringmapper.ValueConverterTask;
import com.knime.bigdata.spark.node.preproc.convert.stringmapperapply.SparkStringMapperApplyTask;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class SparkStringMapperApplyJobTest extends SparkWithJobServerSpec {

    private static String getParams(final String aInputDataPath, final String aMappingTableName,
        final Integer[] aColIdxes, final String[] aColNames, final String aOutputDataPath) {
        return SparkStringMapperApplyTask.paramDef(aInputDataPath, aMappingTableName, aColIdxes, aColNames,
            aOutputDataPath);
    }

    @Test
    public void jobValidationShouldCheckMissingInputDataParameter() throws Throwable {
        String params = getParams(null, "mapTab", new Integer[]{1, 5, 2, 7}, new String[]{"a", "b", "c", "d"}, "tab1");
        myCheck(params, ParameterConstants.PARAM_TABLE_1, "Input");
    }

    @Test
    public void jobValidationShouldCheckMissingMappingTableParameter() throws Throwable {
        String params = getParams("xx", null, new Integer[]{9}, new String[]{"a", "b", "c", "d"}, "tab1");
        myCheck(params, ParameterConstants.PARAM_TABLE_2, "Input");
    }

    @Test
    public void jobValidationShouldCheckMissingColSelectionParameter() throws Throwable {
        String params = getParams("tab1", "mapTab", null, new String[]{"a", "b", "c", "d"}, "tab1");
        myCheck(params, "Input parameter '" + ParameterConstants.PARAM_COL_IDXS
            + "' is not of expected type 'integer list'.");
    }

    //not sure whether we really want to disallow empty column selection...
    //@Test
    public void jobValidationShouldCheckIncorrectColSelectionParameter() throws Throwable {
        String params = getParams("tab1", "mapTab", new Integer[]{}, new String[]{"a", "b", "c", "d"}, "tab2");
        String msg = "Input parameter '" + ParameterConstants.PARAM_COL_IDXS + "' is empty.";
        myCheck(params, msg);

    }

    @Test
    public void jobValidationShouldCheckMissingOuputParameter1() throws Throwable {
        String params = getParams("tab1", "mapTab", new Integer[]{1, 5, 2}, new String[]{"a", "b", "c", "d"}, null);
        myCheck(params, ParameterConstants.PARAM_TABLE_1, "Output");

    }

    private void myCheck(final String params, final String aParam, final String aPrefix) {
        myCheck(params, aPrefix + " parameter '" + aParam + "' missing.");
    }

    private void myCheck(final String params, final String aMsg) {
        KnimeSparkJob testObj = new ApplyNominalValueMappingJob();
        JobConfig config = new JobConfig(ConfigFactory.parseString(params));
        assertEquals("Configuration should be recognized as invalid", ValidationResultConverter.invalid(aMsg),
            testObj.validate(config));
    }

    @Test
    public void runningConverterJobDirectlyShouldProduceResult() throws Throwable {
        ImportKNIMETableJobTest.importTestTable(CONTEXT_ID, ImportKNIMETableJobTest.TEST_TABLE, "tabUnnorm");

        //need to run normalize job first to create mapping...
        final String normParams = ValueConverterTask.paramDef(new Integer[] {1, 3}, new String[] {"a", "c"}, MappingType.COLUMN.toString(), "tabUnnorm", "tabNorm", "mapTab");
        String jobId0 = JobControler.startJob(CONTEXT_ID, ConvertNominalValuesJob.class.getCanonicalName(), normParams);
        JobControler.waitForJobAndFetchResult(CONTEXT_ID, jobId0, null);

        String params = getParams("tabNorm", "mapTab", new Integer[]{1, 3}, new String[]{"a", "c"}, "normalizedData");

        String jobId = JobControler.startJob(CONTEXT_ID, ApplyNominalValueMappingJob.class.getCanonicalName(), params);

        JobResult res = JobControler.waitForJobAndFetchResult(CONTEXT_ID, jobId, null);
        assertFalse("job should have finished properly", res.isError());

        checkResult(CONTEXT_ID, "normalizedData");

    }

    private void checkResult(final KNIMESparkContext aContextName, final String aTableName) throws Exception {

        // now check result:
        String takeJobId =
            JobControler.startJob(aContextName, FetchRowsJob.class.getCanonicalName(), rowFetcherDef(10, aTableName));

        JobResult res = JobControler.waitForJobAndFetchResult(aContextName, takeJobId, null);
        assertNotNull("row fetcher must return a result", res);
        Object[][] arrayRes = (Object[][])res.getObjectResult();
        assertEquals("fetcher should return correct number of rows", ImportKNIMETableJobTest.TEST_TABLE.length,
            arrayRes.length);
        for (int i = 0; i < arrayRes.length; i++) {
            Object[] o = arrayRes[i];
            assertEquals("2x normalize should add 4 columns", ImportKNIMETableJobTest.TEST_TABLE[i].length + 4, o.length);
            assertEquals("normalize and apply normalize should yield same result (Col 1)", (double)o[4], (double)o[6], 0.00000001d);
            assertEquals("normalize and apply normalize should yield same result (Col 3)", (double)o[5], (double)o[7], 0.00000001d);
        }

    }

    private String rowFetcherDef(final int aNumRows, final String aTableName) {
        return JsonUtils.asJson(new Object[]{
            ParameterConstants.PARAM_INPUT,
            new String[]{ParameterConstants.PARAM_NUMBER_ROWS, "" + aNumRows, ParameterConstants.PARAM_TABLE_1,
                aTableName}});
    }

}