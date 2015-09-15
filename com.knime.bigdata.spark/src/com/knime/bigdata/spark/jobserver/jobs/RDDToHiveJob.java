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
 *   Created on Feb 13, 2015 by koetter
 */
package com.knime.bigdata.spark.jobserver.jobs;

import java.io.Serializable;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.api.java.StructType;
import org.apache.spark.sql.hive.api.java.JavaHiveContext;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.jobserver.server.transformation.StructTypeBuilder;

/**
 * Converts the given named RDD into a Hive table.
 *
 * @author Tobias Koetter, KNIME.com
 */
public class RDDToHiveJob extends KnimeSparkJob implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String PARAM_DATA_SCHEMA = ParameterConstants.PARAM_SCHEMA;

    private final static Logger LOGGER = Logger.getLogger(RDDToHiveJob.class.getName());

    /**
     * parse parameters - there are no default values, all values are required
     *
     */
    @Override
    public SparkJobValidation validate(final JobConfig config) {
        String msg = null;
        if (!config.hasInputParameter(PARAM_INPUT_TABLE)) {
            msg = "Input parameter '" + PARAM_INPUT_TABLE + "' missing.";
        }
        if (msg == null && !config.hasInputParameter(PARAM_DATA_SCHEMA)) {
            msg = "Input parameter '" + PARAM_DATA_SCHEMA + "' missing.";
        }
        if (msg == null && !config.hasOutputParameter(PARAM_RESULT_TABLE)) {
            msg = "Output parameter '" + PARAM_RESULT_TABLE + "' missing.";
        }
        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }
        return ValidationResultConverter.valid();
    }

    /**
     * run the actual job, the result is serialized back to the client the true result is a Hive data table that
     * contains the data of the incoming rdd
     *
     * @return the JobResult
     */
    @Override
    public JobResult runJobWithContext(final SparkContext sc, final JobConfig aConfig)
        throws GenericKnimeSparkException {
        LOGGER.log(Level.INFO, "writing hive table...");
        final JavaRDD<Row> rowRDD = getFromNamedRdds(aConfig.getInputParameter(PARAM_INPUT_TABLE));
        final String schemaString = aConfig.getInputParameter(PARAM_DATA_SCHEMA);
        final StructType resultSchema = StructTypeBuilder.fromConfigString(schemaString);
        final String hiveTableName = aConfig.getOutputStringParameter(PARAM_RESULT_TABLE);
        try {
            final JavaHiveContext hiveContext = new JavaHiveContext(JavaSparkContext.fromSparkContext(sc));
            final JavaSchemaRDD schemaPredictedData = hiveContext.applySchema(rowRDD, resultSchema);
            schemaPredictedData.saveAsTable(hiveTableName);
        } catch (Exception e) {
            String msg = "Failed to create hive table with name '" + hiveTableName + "'. Exception: ";
            //requires import of hadoop stuff
            //            if (e instanceof AlreadyExistsException) {
            //                throw new GenericKnimeSparkException(msg + "Table already exists");
            //            }
            throw new GenericKnimeSparkException(msg + e.getMessage());
        }
        return JobResult.emptyJobResult().withMessage("OK");
    }
}
