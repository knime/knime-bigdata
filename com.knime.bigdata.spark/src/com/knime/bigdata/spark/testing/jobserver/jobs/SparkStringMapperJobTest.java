package com.knime.bigdata.spark.testing.jobserver.jobs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JobStatus;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.client.KnimeContext;
import com.knime.bigdata.spark.jobserver.jobs.ConvertNominalValuesJob;
import com.knime.bigdata.spark.jobserver.jobs.FetchRowsJob;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.MappingType;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class SparkStringMapperJobTest {

    static String getInputOutputParamPair(final String aInputDataPath, final String aQualityMeasure,
        final Integer[] aColIdxes, final String aOutputDataPath1, final String aOutputDataPath2) {
        StringBuilder params = new StringBuilder("");
        params.append("   \"").append(ParameterConstants.PARAM_INPUT).append("\" {\n");

        if (aInputDataPath != null) {
            params.append("         \"").append(ParameterConstants.PARAM_TABLE_1).append("\": \"")
                .append(aInputDataPath).append("\",\n");
        }
        if (aQualityMeasure != null) {
            params.append("         \"").append(ParameterConstants.PARAM_STRING).append("\": \"")
                .append(aQualityMeasure).append("\",\n");
        }
        if (aColIdxes != null) {
            params.append("         \"").append(ParameterConstants.PARAM_COL_IDXS).append("\": ")
                .append(JsonUtils.toJsonArray((Object[])aColIdxes)).append(",\n");
        }
        params.append("    }\n");
        params.append("    \"").append(ParameterConstants.PARAM_OUTPUT).append("\" {\n");
        if (aOutputDataPath1 != null) {
            params.append("         \"").append(ParameterConstants.PARAM_TABLE_1).append("\": \"")
                .append(aOutputDataPath1).append("\"\n");
        }
        if (aOutputDataPath2 != null) {
            params.append("         \"").append(ParameterConstants.PARAM_TABLE_2).append("\": \"")
                .append(aOutputDataPath2).append("\"\n");
        }
        params.append("    }\n");
        params.append("    \n");
        return params.toString();
    }

    private static String getParams(final String aInputDataPath, final String aQualityMeasure, final Integer[] aColIdxes,
        final String aOutputDataPath1, final String aOutputDataPath2) {
        StringBuilder params = new StringBuilder("{\n");
        params.append(getInputOutputParamPair(aInputDataPath, aQualityMeasure, aColIdxes, aOutputDataPath1,
            aOutputDataPath2));
        params.append("}");
        return params.toString();
    }

    @Test
    public void jobValidationShouldCheckMissingInputDataParameter() throws Throwable {
        String params = getParams(null, MappingType.COLUMN.toString(),new Integer[] {1,5,2,7}, "tab1", "tab2");
        myCheck(params, ParameterConstants.PARAM_INPUT + "." + ParameterConstants.PARAM_TABLE_1, "Input");
    }

    @Test
    public void jobValidationShouldCheckMissingQualityMeasureParameter() throws Throwable {
        String params = getParams("xx", null, new Integer[] {9}, "tab1", "tab2");
        myCheck(params, ParameterConstants.PARAM_INPUT + "." + ParameterConstants.PARAM_STRING, "Input");
    }

    @Test
    public void jobValidationShouldCheckIncorrectQualityMeasureParameter() throws Throwable {
        String params = getParams("xx", "notproper", new Integer[] {99}, "tab1", "tab2");
        String msg =
                "Input parameter '" + ParameterConstants.PARAM_INPUT + "." + ParameterConstants.PARAM_STRING
                    + "' has an invalid value.";
        myCheck(params,msg);
    }

    @Test
    public void jobValidationShouldCheckMissingColSelectionParameter() throws Throwable {
        String params = getParams("tab1", MappingType.COLUMN.toString(), null , "tab1", "tab2");
        myCheck(params, ParameterConstants.PARAM_INPUT + "." + ParameterConstants.PARAM_COL_IDXS, "Input");
    }

    @Test
    public void jobValidationShouldCheckIncorrectColSelectionParameter() throws Throwable {
        String params = getParams("tab1", MappingType.COLUMN.toString(), new Integer[] {}, "tab1", "tab2");
        String msg =
            "Input parameter '" + ParameterConstants.PARAM_INPUT + "." + ParameterConstants.PARAM_COL_IDXS
                + "' is empty.";
        myCheck(params, msg);

    }

    @Test
    public void jobValidationShouldCheckMissingOuputParameter1() throws Throwable {
        String params = getParams("tab1", MappingType.COLUMN.toString(), new Integer[] {1,5,2}, null, "tab2");
        myCheck(params, ParameterConstants.PARAM_OUTPUT + "." + ParameterConstants.PARAM_TABLE_1, "Output");

    }

    @Test
    public void jobValidationShouldCheckMissingOuputParameter2() throws Throwable {
        String params = getParams("tab1", MappingType.COLUMN.toString(), new Integer[] {1,5,2}, "tab1", null);
        myCheck(params, ParameterConstants.PARAM_OUTPUT + "." + ParameterConstants.PARAM_TABLE_2, "Output");

    }

    private void myCheck(final String params, final String aParam, final String aPrefix) {
        myCheck(params, aPrefix + " parameter '" + aParam + "' missing.");
    }

    private void myCheck(final String params, final String aMsg) {
        ConvertNominalValuesJob testObj = new ConvertNominalValuesJob();
        Config config = ConfigFactory.parseString(params);
        assertEquals("Configuration should be recognized as invalid", ValidationResultConverter.invalid(aMsg),
            testObj.validate(config));
    }

    //@Test
    public void runningConverterJobDirectlyShouldProduceResult() throws Throwable {
        KNIMESparkContext contextName = KnimeContext.getSparkContext();
        try {

            String params = getParams("tab1", MappingType.COLUMN.toString(), new Integer[] {1,5,2}, "tab1", "tab2");

            String jobId =
                JobControler.startJob(contextName, ConvertNominalValuesJob.class.getCanonicalName(), params.toString());

            assertFalse("job should have finished properly",
                JobControler.waitForJob(contextName, jobId, null).equals(JobStatus.UNKNOWN));

            // result is serialized as a string
            assertFalse("job should not be running anymore",
                JobStatus.OK.equals(JobControler.getJobStatus(contextName, jobId)));

            checkResult(contextName);

        } finally {
            KnimeContext.destroySparkContext(contextName);
        }

    }

    private void checkResult(final KNIMESparkContext aContextName) throws Exception {

        // now check result:
        String takeJobId =
            JobControler.startJob(aContextName, FetchRowsJob.class.getCanonicalName(),
                rowFetcherDef(10, "/home/spark/..."));
        assertFalse("job should have finished properly",
            JobControler.waitForJob(aContextName, takeJobId, null).equals(JobStatus.UNKNOWN));
        JobResult res = JobControler.fetchJobResult(aContextName, takeJobId);
        assertNotNull("row fetcher must return a result", res);
        assertEquals("fetcher should return OK as result status", "OK", res.getMessage());
        Object[][] arrayRes = (Object[][])res.getObjectResult();
        assertEquals("fetcher should return correct number of rows", 10, arrayRes.length);
        for (int i = 0; i < arrayRes.length; i++) {
            Object[] o = arrayRes[i];
            System.out.println("row[" + i + "]: " + o);
        }
    }

    private String rowFetcherDef(final int aNumRows, final String aTableName) {
        return JsonUtils.asJson(new Object[]{
            ParameterConstants.PARAM_INPUT,
            new String[]{ParameterConstants.PARAM_NUMBER_ROWS, "" + aNumRows, ParameterConstants.PARAM_TABLE_1,
                aTableName}});
    }

}