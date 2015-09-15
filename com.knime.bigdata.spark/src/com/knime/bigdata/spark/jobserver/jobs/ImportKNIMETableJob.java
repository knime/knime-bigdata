/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 *
 * History
 *   Created on 24.07.2015 by dwk
 */
package com.knime.bigdata.spark.jobserver.jobs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.Row;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.jobserver.server.transformation.RowBuilder;

/**
 *
 * @author dwk
 */
public class ImportKNIMETableJob extends KnimeSparkJob implements Serializable {

    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(ImportKNIMETableJob.class.getName());

    /**
     * parse parameters - there are no default values, all values are required
     *
     */
    @Override
    public SparkJobValidation validate(final JobConfig aConfig) {
        String msg = null;

        if (!aConfig.hasInputParameter(PARAM_INPUT_TABLE)) {
            msg = "Input parameter '" + PARAM_INPUT_TABLE + "' missing.";
        }

        if (msg == null && !aConfig.hasOutputParameter(PARAM_RESULT_TABLE)) {
            msg = "Output parameter '" + PARAM_RESULT_TABLE + "' missing.";
        }

        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }
        return ValidationResultConverter.valid();
    }

    /**
     * convert object array in config object to List<Row>
     *
     * @param aConfig
     * @return data as list of Row objects
     * @throws GenericKnimeSparkException
     */
    public static List<Row> getInputData(final JobConfig aConfig) throws GenericKnimeSparkException {
        final List<Row> rows = new ArrayList<>();
        final Object[][] data = aConfig.readInputFromFileAndDecode(PARAM_INPUT_TABLE);
        if (data == null) {
            throw new GenericKnimeSparkException("Input parameter '" + PARAM_INPUT_TABLE + "' is empty' ");
        }
        for (Object[] rowData : data) {
            RowBuilder row = RowBuilder.emptyRow();

            for (int ix = 0; ix < rowData.length; ix++) {
                if (rowData[ix] == null) {
                    row.add(null);
                } else {
                    row.add(rowData[ix]);
                }
            }
            rows.add(row.build());
        }
        return rows;
    }

    /**
     * run the actual job, the result is serialized back to the client the true result is stored in the map of named
     * RDDs
     *
     * @return "OK"
     * @throws GenericKnimeSparkException
     */
    @Override
    public JobResult runJobWithContext(final SparkContext sc, final JobConfig aConfig)
        throws GenericKnimeSparkException {
        LOGGER.log(Level.INFO, "inserting KNIME data table into RDD...");
        final List<Row> rowData = getInputData(aConfig);
        storeInRdd(sc, rowData, aConfig.getOutputStringParameter(PARAM_RESULT_TABLE));
        return JobResult.emptyJobResult().withMessage("OK");
    }

    /**
     * stores the data in a RDD
     *
     *
     * @param aSparkContext
     * @param aData
     * @param aRddName
     */
    private void storeInRdd(final SparkContext aSparkContext, final List<Row> aData, final String aRddName) {
        @SuppressWarnings("resource")
        JavaSparkContext javaContext = new JavaSparkContext(aSparkContext);

        JavaRDD<Row> rdd = javaContext.parallelize(aData);
        LOGGER.log(Level.INFO, "Storing data rdd under key: " + aRddName);
        addToNamedRdds(aRddName, rdd);
    }
}
