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
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

import com.knime.bigdata.spark.jobserver.server.CollaborativeFilteringModel;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ModelUtils;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.SupervisedLearnerUtils;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;

import spark.jobserver.SparkJobValidation;

/**
 * applies previously learned MLlib model to given RDD, predictions are inserted into a new RDD and (temporarily)
 * stored in the map of named RDDs, optionally saved to disk
 *
 * @author Tobias Koetter, KNIME.com, dwk
 */
public class MLlibPredictorJob extends KnimeSparkJob implements Serializable {

    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(MLlibPredictorJob.class.getName());

    /**
     * parse parameters - there are no default values, but two required values: - the kmeans model - the input JavaRDD
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
        if (msg == null && !aConfig.hasInputParameter(ParameterConstants.PARAM_MODEL_NAME)) {
            msg = "Input model missing!";
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

        LOGGER.log(Level.INFO, "starting prediction job...");
        final JavaRDD<Row> rowRDD = getFromNamedRdds(aConfig.getInputParameter(PARAM_INPUT_TABLE));

        final List<Integer> colIdxs = SupervisedLearnerUtils.getSelectedColumnIds(aConfig);

        final Serializable model = aConfig.readInputFromFileAndDecode(ParameterConstants.PARAM_MODEL_NAME);
        final JavaRDD<Row> predictions;

        if (model instanceof CollaborativeFilteringModel) {
            //this is a very special model as we need to convert it to the real model first
            predictions = CollaborativeFilteringJob.predict(this, aConfig, rowRDD, colIdxs, (CollaborativeFilteringModel)model);
        } else {
            predictions = ModelUtils.predict(aConfig, rowRDD, colIdxs, model);
        }


        LOGGER.log(Level.INFO, "Prediction done");
        addToNamedRdds(aConfig.getOutputStringParameter(PARAM_RESULT_TABLE), predictions);
        return JobResult.emptyJobResult().withMessage("OK");

    }
}
