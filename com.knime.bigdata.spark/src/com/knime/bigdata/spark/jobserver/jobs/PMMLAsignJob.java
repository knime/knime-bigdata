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
 *   Created on 29.09.2015 by koetter
 */
package com.knime.bigdata.spark.jobserver.jobs;

import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.Row;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.SupervisedLearnerUtils;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public abstract class PMMLAsignJob extends KnimeSparkJob {

    private static final String PARAM_MAIN_CLASS = ParameterConstants.PARAM_MAIN_CLASS;
    private static final Logger LOGGER = Logger.getLogger(PMMLAsignJob.class.getName());

    /**
     *
     */
    public PMMLAsignJob() {
        super();
    }

    /**
     * parse parameters
     */
    @Override
    public SparkJobValidation validate(final JobConfig aConfig) {
        String msg = null;
        if (!aConfig.hasInputParameter(PARAM_INPUT_TABLE)) {
            msg = "Input parameter '" + PARAM_INPUT_TABLE + "' missing.";
        }

        if (msg == null) {
            msg = SupervisedLearnerUtils.checkSelectedColumnIdsParameter(aConfig);
        }

        if (msg == null && !aConfig.hasOutputParameter(PARAM_RESULT_TABLE)) {
            msg = "Output parameter '" + PARAM_RESULT_TABLE + "' missing.";
        }
        if (msg == null && !aConfig.hasInputParameter(PARAM_MAIN_CLASS)) {
            msg = "Main class name missing!";
        }
        if (msg == null && !aConfig.hasInputParameter(ParameterConstants.PARAM_MODEL_NAME)) {
            msg = "Compiled PMML model missing!";
        }
        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }
        return ValidationResultConverter.valid();
    }

    private void validateInput(final JobConfig aConfig) throws GenericKnimeSparkException {
        String msg = null;
        if (!validateNamedRdd(aConfig.getInputParameter(PARAM_INPUT_TABLE))) {
            msg = "Input data table missing!";
        }
        if (msg != null) {
            LOGGER.severe(msg);
            throw new GenericKnimeSparkException(GenericKnimeSparkException.ERROR + ":" + msg);
        }
    }

    /**
     * run the actual job, the result is serialized back to the client
     *
     * @return "OK" - the actual predictions are stored in a named rdd and can be retrieved by a separate job or used
     *         later on
     * @throws GenericKnimeSparkException
     */
    @Override
    public JobResult runJobWithContext(final SparkContext sc, final JobConfig aConfig) throws GenericKnimeSparkException {
        validateInput(aConfig);
        LOGGER.log(Level.FINE, "starting pmml asignment job...");
        final JavaRDD<Row> rowRDD = getFromNamedRdds(aConfig.getInputParameter(PARAM_INPUT_TABLE));
        final List<Integer> inputColIdxs = SupervisedLearnerUtils.getSelectedColumnIds(aConfig);
        final String mainClass = aConfig.getInputParameter(PARAM_MAIN_CLASS);
        final Map<String, byte[]> bytecode = aConfig.readInputFromFileAndDecode(ParameterConstants.PARAM_MODEL_NAME);
        try {
            Function<Row, Row> asign = createFunction(bytecode, mainClass, inputColIdxs, aConfig);
            final JavaRDD<Row> resultRDD = rowRDD.map(asign);
            addToNamedRdds(aConfig.getOutputStringParameter(PARAM_RESULT_TABLE), resultRDD);
            LOGGER.log(Level.FINE, "pmml prediction done");
            return JobResult.emptyJobResult().withMessage("OK")
                .withTable(aConfig.getOutputStringParameter(PARAM_RESULT_TABLE), null);
        } catch (Exception e) {
            final String msg = "Exception in pmml asignment job: " + e.getMessage();
            LOGGER.log(Level.SEVERE, msg, e);
            throw new GenericKnimeSparkException(msg, e);
        }
    }

    /**
     * @param inputColIdxs
     * @param mainClass
     * @param bytecode
     * @param aConfig
     * @return the pre
     */
    protected abstract Function<Row, Row> createFunction(final Map<String, byte[]> bytecode, final String mainClass,
        final List<Integer> inputColIdxs, JobConfig aConfig);
}