package com.knime.bigdata.spark.jobserver.jobs;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.spark.sql.api.java.Row;
import org.junit.Test;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.jobserver.server.transformation.RowBuilder;
import com.knime.bigdata.spark.node.io.table.writer.Table2SparkNodeModel;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class ImportKNIMETableJobParametersTest {

    @Test(expected = GenericKnimeSparkException.class)
    public void jobValidationShouldCheckMissingInputDataParameter() throws Throwable {
        String params =
                Table2SparkNodeModel.paramDef(null, "OutTab");
        JobConfig config = new JobConfig(ConfigFactory.parseString(params));
        ImportKNIMETableJob.getInputData(config);
    }

    @Test
    public void jobValidationShouldCheckMissingOuputParameter() throws Throwable {
        String params =
                Table2SparkNodeModel.paramDef(new Object[][]{new Object[]{1, true, 3.2d, "my string"}},  null);
        myCheck(params, KnimeSparkJob.PARAM_RESULT_TABLE, "Output");
    }

    @Test
    public void jobValidationShouldCheckAllValidParams() throws Throwable {
        String params =
                Table2SparkNodeModel.paramDef(new Object[][]{new Object[]{1, true, 3.2d, "my string"}},  "outtab");
        KnimeSparkJob testObj = new ImportKNIMETableJob();
        JobConfig config = new JobConfig(ConfigFactory.parseString(params));
        assertEquals("Configuration should be recognized as valid", ValidationResultConverter.valid(),
            testObj.validate(config));
    }

    private void myCheck(final String params, final String aParam, final String aPrefix) {
        myCheck(params, aPrefix + " parameter '" + aParam + "' missing.");
    }

    private void myCheck(final String params, final String aMsg) {
        KnimeSparkJob testObj = new ImportKNIMETableJob();
        JobConfig config = new JobConfig(ConfigFactory.parseString(params));
        assertEquals("Configuration should be recognized as invalid", ValidationResultConverter.invalid(aMsg),
            testObj.validate(config));
    }

    @Test
    public void gernerateListOfRowFromConfigObject() throws Throwable {
        String params =
                Table2SparkNodeModel.paramDef(new Object[][]{new Object[]{1, true, 3.2d, "my string"},
                new Object[]{2, false, 6.2d, "my other string"}},  "outtab");
        JobConfig config = new JobConfig(ConfigFactory.parseString(params));
        List<Row> rows = ImportKNIMETableJob.getInputData(config);
        assertEquals("conversion of data table failed for Row 0",
            RowBuilder.emptyRow().add(1).add(true).add(3.2d).add("my string").build(), rows.get(0));
        assertEquals("conversion of data table failed for Row 1",
            RowBuilder.emptyRow().add(2).add(false).add(6.2d).add("my other string").build(), rows.get(1));
    }

}