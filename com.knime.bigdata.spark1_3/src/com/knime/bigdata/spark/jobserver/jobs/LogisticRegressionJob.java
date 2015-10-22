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
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;
import org.apache.spark.mllib.regression.GeneralizedLinearAlgorithm;
import org.apache.spark.mllib.regression.LabeledPoint;

import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;

import spark.jobserver.SparkJobValidation;

/**
 * runs MLlib logistic regression on a given RDD to create a classification model
 *
 * @author koetter, dwk
 */
public class LogisticRegressionJob extends AbstractRegularizationJob implements Serializable {

    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(LogisticRegressionJob.class.getName());

    /**
     * use SGD or LBFGS optimization
     */
    public static final String PARAM_USE_SGD = "useSGD";

    /**
     * number of corrections (for LBFGS optimization)
     */
    public static final String PARAM_NUM_CORRECTIONS = "numCorrections";

    /**
     * tolerance (for LBFGS optimization)
     */
    public static final String PARAM_TOLERANCE = "tolerance";

    /**
     * {@inheritDoc}
     */
    @Override
    Logger getLogger() {
        return LOGGER;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    String getAlgName() {
        return "Logistic Regression (with SGD or LBFGS)";
    }

    @Override
    public SparkJobValidation validate(final JobConfig aConfig) {
        String msg = null;
        if (!aConfig.hasInputParameter(PARAM_USE_SGD)) {
            msg = "Input parameter '" + PARAM_USE_SGD + "' missing.";
        }

        if (msg == null) {
            if (useSGD(aConfig)) {
                if (!aConfig.hasInputParameter(PARAM_STEP_SIZE)) {
                    msg = "Input parameter '" + PARAM_STEP_SIZE + "' missing.";
                }
                if (msg == null && !aConfig.hasInputParameter(PARAM_FRACTION)) {
                    msg = "Input parameter '" + PARAM_FRACTION + "' missing.";
                }
            } else {
                if (!aConfig.hasInputParameter(PARAM_NUM_CORRECTIONS)) {
                    msg = "Input parameter '" + PARAM_NUM_CORRECTIONS + "' missing.";
                }
                if (msg == null && !aConfig.hasInputParameter(PARAM_TOLERANCE)) {
                    msg = "Input parameter '" + PARAM_TOLERANCE + "' missing.";
                }
            }
        }

        if (msg != null) {
            return ValidationResultConverter.invalid(msg);
        }

        return super.validate(aConfig);
    }

    /**
     * Train logistic regression
     *
     * @param aConfig
     * @param aInputData - Training dataset: RDD of LabeledPoint.
     * @return LogisticRegressionModel
     */
    @Override
    public LogisticRegressionModel execute(final SparkContext aContext, final JobConfig aConfig, final JavaRDD<LabeledPoint> aInputData) {
        final GeneralizedLinearAlgorithm<LogisticRegressionModel> alg;
        if (useSGD(aConfig)) {
            alg = configureLogRegWithSGD(aConfig);
        } else {
            alg = configureLogRegWithLBFGS(aConfig);
        }

        return alg.run(aInputData.rdd().cache());
    }

    static GeneralizedLinearAlgorithm<LogisticRegressionModel> configureLogRegWithSGD(final JobConfig aConfig) {
        final LogisticRegressionWithSGD alg = new LogisticRegressionWithSGD();
        configureSGD(aConfig, alg);
        return alg;
    }

    /**
     * @param alg
     * @param aConfig
     */
    static void configureSGD(final JobConfig aConfig, final LogisticRegressionWithSGD alg) {
        configureSGDOptimizer(aConfig, alg.optimizer());
        configureAlgorithm(aConfig, alg);
    }

    /**
     * @param alg
     * @param aConfig
     */
    static void configureAlgorithm(final JobConfig aConfig,
        final GeneralizedLinearAlgorithm<LogisticRegressionModel> alg) {
        alg.setFeatureScaling(getFeatureScaling(aConfig)).setIntercept(getIntercept(aConfig))
            .setValidateData(getValidateData(aConfig));
    }

    static GeneralizedLinearAlgorithm<LogisticRegressionModel> configureLogRegWithLBFGS(final JobConfig aConfig) {
        final LogisticRegressionWithLBFGS alg = new LogisticRegressionWithLBFGS();
        alg.optimizer().setNumIterations(getNumIterations(aConfig)).setRegParam(getRegularization(aConfig))
            .setUpdater(getUpdater(aConfig)).setGradient(getGradient(aConfig));

        configureAlgorithm(aConfig, alg);
        alg.optimizer().setConvergenceTol(getTolerance(aConfig)).setNumCorrections(getNumCorrections(aConfig));

        return alg;
    }

    /**
     * @param aConfig
     * @return
     */
    static Boolean useSGD(final JobConfig aConfig) {
        return aConfig.getInputParameter(PARAM_USE_SGD, Boolean.class);
    }

    /**
     * @param aConfig
     * @return
     */
    static Integer getNumCorrections(final JobConfig aConfig) {
        return aConfig.getInputParameter(PARAM_NUM_CORRECTIONS, Integer.class);
    }

    /**
     * @param aConfig
     * @return
     */
    static Double getTolerance(final JobConfig aConfig) {
        return aConfig.getInputParameter(PARAM_TOLERANCE, Double.class);
    }
}
