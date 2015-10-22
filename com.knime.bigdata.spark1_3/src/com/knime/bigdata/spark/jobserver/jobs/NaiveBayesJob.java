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
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.Row;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.RDDUtils;
import com.knime.bigdata.spark.jobserver.server.SupervisedLearnerUtils;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;

import spark.jobserver.SparkJobValidation;

/**
 * runs MLlib Naive Bayes on a given RDD
 *
 * @author koetter, dwk
 */
public class NaiveBayesJob extends KnimeSparkJob implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The smoothing parameter
     */
    public static final String PARAM_LAMBDA = "Lambda";

    private final static Logger LOGGER = Logger.getLogger(NaiveBayesJob.class.getName());

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

        if (msg == null) {
            if (!aConfig.hasInputParameter(PARAM_LAMBDA)) {
                msg = "Input parameter '" + PARAM_LAMBDA + "' missing.";
            } else {
                try {
                    if (aConfig.getInputParameter(PARAM_LAMBDA, Double.class) == null) {
                        msg = "Input parameter '" + PARAM_LAMBDA + "' is empty.";
                    }
                } catch (Exception e) {
                    msg = "Input parameter '" + PARAM_LAMBDA + "' is not of expected type 'double'.";
                }
            }
        }
        if (msg == null) {
            msg = SupervisedLearnerUtils.checkLableColumnParameter(aConfig);
        }

        if (msg == null) {
            msg = SupervisedLearnerUtils.checkSelectedColumnIdsParameter(aConfig);
        }

        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }
        return ValidationResultConverter.valid();
    }

    /**
     * run the actual job, the result is serialized back to the client
     */
    @Override
    public JobResult runJobWithContext(final SparkContext sc, final JobConfig aConfig)
        throws GenericKnimeSparkException {
        SupervisedLearnerUtils.validateInput(aConfig, this, LOGGER);
        LOGGER.log(Level.INFO, "starting Naive Bayes learner job...");
        final JavaRDD<Row> rowRDD =
            getFromNamedRdds(aConfig.getInputParameter(PARAM_INPUT_TABLE));
        final JavaRDD<LabeledPoint> inputRdd = SupervisedLearnerUtils.getTrainingData(aConfig, rowRDD);

        final Serializable model = execute(sc, aConfig, inputRdd);

        JobResult res = JobResult.emptyJobResult().withMessage("OK").withObjectResult(model);

        if (aConfig.hasOutputParameter(PARAM_RESULT_TABLE)) {
            SupervisedLearnerUtils.storePredictions(sc, aConfig, this, rowRDD,
                RDDUtils.toVectorRDDFromLabeledPointRDD(inputRdd), model, LOGGER);
        }
        LOGGER.log(Level.INFO, " Naive Bayes Learner done");
        // note that with Spark 1.4 we can use PMML instead
        return res;

    }

    /**
     * @param aConfig
     */
    static Double getLambda(final JobConfig aConfig) {
        return aConfig.getInputParameter(PARAM_LAMBDA, Double.class);
    }

    /**
     *
     * @param aContext
     * @param aConfig
     * @param aInputData - Training dataset: RDD of LabeledPoint. Labels should take values {0, 1, ..., numClasses-1}.
     * @return model
     */
    static NaiveBayesModel execute(final SparkContext aContext, final JobConfig aConfig,
        final JavaRDD<LabeledPoint> aInputData) {
        aInputData.cache();
        return NaiveBayes.train(aInputData.rdd(), getLambda(aConfig));
    }

}
