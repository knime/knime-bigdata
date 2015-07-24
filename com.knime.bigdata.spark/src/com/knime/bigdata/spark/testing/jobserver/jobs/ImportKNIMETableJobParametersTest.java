package com.knime.bigdata.spark.testing.jobserver.jobs;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.spark.sql.api.java.Row;
import org.junit.Test;

import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.jobs.ImportKNIMETableJob;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.jobserver.server.transformation.RowBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class ImportKNIMETableJobParametersTest {

    private static String getParams(final Object[][] data, final Class<?>[] aPrimitiveTypes,
        final String aResultTableName) {
        StringBuilder params = new StringBuilder("{\n");
        params.append("   \"").append(ParameterConstants.PARAM_INPUT).append("\" {\n");

        if (data != null) {
            params.append("         \"").append(ParameterConstants.PARAM_TABLE_1).append("\": ")
                .append(JsonUtils.toJson2DimArray(data)).append(",\n");
        }
        if (aPrimitiveTypes != null) {
            params.append("         \"").append(ParameterConstants.PARAM_SCHEMA).append("\": ")
                .append(JsonUtils.toJsonArray(aPrimitiveTypes)).append(",\n");
        }

        params.append("    }\n");
        params.append("    \"").append(ParameterConstants.PARAM_OUTPUT).append("\" {\n");
        if (aResultTableName != null) {
            params.append("         \"").append(ParameterConstants.PARAM_TABLE_1).append("\": \"")
                .append(aResultTableName).append("\"\n");
        }
        params.append("    }\n");
        params.append("    \n}");
        return params.toString();
    }

    @Test
    public void jobValidationShouldCheckMissingInputDataParameter() throws Throwable {
        String params =
            getParams(null, new Class<?>[]{Integer.class, Boolean.class, Double.class, String.class}, "OutTab");
        myCheck(params, ParameterConstants.PARAM_INPUT + "." + ParameterConstants.PARAM_TABLE_1, "Input");
    }

    @Test
    public void jobValidationShouldCheckMissingColumnClassParameter() throws Throwable {
        String params = getParams(new Object[][]{new Object[]{1, true, 3.2d, "my string"}}, null, "OutTab");
        myCheck(params, ParameterConstants.PARAM_INPUT + "." + ParameterConstants.PARAM_SCHEMA, "Input");
    }

    @Test
    public void jobValidationShouldCheckMissingOuputParameter() throws Throwable {
        String params =
            getParams(new Object[][]{new Object[]{1, true, 3.2d, "my string"}}, new Class<?>[]{Integer.class,
                Boolean.class, Double.class, String.class}, null);
        myCheck(params, ParameterConstants.PARAM_OUTPUT + "." + ParameterConstants.PARAM_TABLE_1, "Output");
    }

    @Test
    public void jobValidationShouldCheckAllValidParams() throws Throwable {
        String params =
            getParams(new Object[][]{new Object[]{1, true, 3.2d, "my string"}}, new Class<?>[]{Integer.class,
                Boolean.class, Double.class, String.class}, "outtab");
        KnimeSparkJob testObj = new ImportKNIMETableJob();
        Config config = ConfigFactory.parseString(params);
        assertEquals("Configuration should be recognized as valid", ValidationResultConverter.valid(),
            testObj.validate(config));
    }

    private void myCheck(final String params, final String aParam, final String aPrefix) {
        myCheck(params, aPrefix + " parameter '" + aParam + "' missing.");
    }

    private void myCheck(final String params, final String aMsg) {
        KnimeSparkJob testObj = new ImportKNIMETableJob();
        Config config = ConfigFactory.parseString(params);
        assertEquals("Configuration should be recognized as invalid", ValidationResultConverter.invalid(aMsg),
            testObj.validate(config));
    }

    @Test
    public void gernerateListOfRowFromConfigObject() throws Throwable {
        String params =
            getParams(new Object[][]{new Object[]{1, true, 3.2d, "my string"},
                new Object[]{2, false, 6.2d, "my other string"}}, new Class<?>[]{Integer.class, Boolean.class,
                Double.class, String.class}, "outtab");
        Config config = ConfigFactory.parseString(params);
        List<Row> rows = ImportKNIMETableJob.getInputData(config);
        assertEquals("conversion of data table failed for Row 0",
            RowBuilder.emptyRow().add(1).add(true).add(3.2d).add("my string").build(), rows.get(0));
        assertEquals("conversion of data table failed for Row 1",
            RowBuilder.emptyRow().add(2).add(false).add(6.2d).add("my other string").build(), rows.get(1));
    }

}