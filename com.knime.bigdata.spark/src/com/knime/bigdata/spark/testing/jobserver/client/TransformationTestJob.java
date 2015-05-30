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
 *   Created on 29.05.2015 by Dietrich
 */
package com.knime.bigdata.spark.testing.jobserver.client;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nonnull;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.api.java.StructType;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.jobserver.server.transformation.InvalidSchemaException;
import com.knime.bigdata.spark.jobserver.server.transformation.StructTypeBuilder;
import com.knime.bigdata.spark.jobserver.server.transformation.UserDefinedTransformation;
import com.typesafe.config.Config;

/**
 *
 * @author dwk
 */
public class TransformationTestJob extends KnimeSparkJob implements UserDefinedTransformation {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private static final String PARAM_INPUT_TABLE_KEY = ParameterConstants.PARAM_INPUT
            + "." + ParameterConstants.PARAM_TABLE_1;

    private static final String PARAM_OUTPUT_TABLE_KEY = ParameterConstants.PARAM_OUTPUT
            + "." + ParameterConstants.PARAM_TABLE_1;

    private final static Logger LOGGER = Logger.getLogger(TransformationTestJob.class
            .getName());

    /**
     * parse parameters - there are no default values, but two required values:
     * - the key of the input JavaRDD
     * - the key of the output JavaRDD
     */
    @Override
    public SparkJobValidation validate(final Config aConfig) {
        String msg = null;
        if (!aConfig.hasPath(PARAM_INPUT_TABLE_KEY)) {
            msg = "Input parameter '" + PARAM_INPUT_TABLE_KEY + "' missing.";
        }
        if (msg == null && !aConfig.hasPath(PARAM_OUTPUT_TABLE_KEY)) {
            msg = "Output parameter '" + PARAM_OUTPUT_TABLE_KEY + "' missing.";
        }
        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }
        return ValidationResultConverter.valid();
    }

    private SparkJobValidation validateInput(final Config aConfig) {
        String msg = null;
        if (!validateNamedRdd(aConfig.getString(PARAM_INPUT_TABLE_KEY))) {
            msg = "Input data table missing for key: "+aConfig.getString(PARAM_INPUT_TABLE_KEY);
        }
        if (msg != null) {
            LOGGER.severe(msg);
            return ValidationResultConverter
                    .invalid(GenericKnimeSparkException.ERROR + ": " + msg);
        }

        return ValidationResultConverter.valid();
    }

    /**
     * run the actual job, the result is serialized back to the client
     * the primary result of this job should be a side effect - new new RDD in the
     * map of named RDDs
     * @return JobResult with table information
     */
    @Override
    protected JobResult runJobWithContext(final SparkContext aSparkContext, final Config aConfig) {
        SparkJobValidation validation = validateInput(aConfig);
        if (!ValidationResultConverter.isValid(validation)) {
            return JobResult.emptyJobResult().withMessage(validation.toString());
        }

        LOGGER.log(Level.INFO, "starting transformation job...");
        final JavaRDD<Row> rowRDD = getFromNamedRdds(aConfig
                .getString(PARAM_INPUT_TABLE_KEY));

        final JavaRDD<Row> transformed = apply(rowRDD, null);

        LOGGER.log(Level.INFO, "transformation completed");
        addToNamedRdds(aConfig.getString(PARAM_OUTPUT_TABLE_KEY), transformed);
        try {
            final StructType schema = StructTypeBuilder.fromRows(transformed.take(10)).build();
            return JobResult.emptyJobResult().withMessage("OK").withTable(aConfig.getString(PARAM_OUTPUT_TABLE_KEY), schema);
        } catch (InvalidSchemaException e) {
            return JobResult.emptyJobResult().withMessage("ERROR: "+e.getMessage());
        }
    }

    @Override
    @Nonnull
    public <T extends JavaRDD<Row>> JavaRDD<Row> apply(@Nonnull final T input, final T aNullInput) {
        return input;
    }
}
