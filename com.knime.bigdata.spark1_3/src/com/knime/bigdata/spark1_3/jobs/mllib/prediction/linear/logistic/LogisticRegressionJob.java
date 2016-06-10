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
package com.knime.bigdata.spark1_3.jobs.mllib.prediction.linear.logistic;

import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;
import org.apache.spark.mllib.regression.GeneralizedLinearAlgorithm;
import org.apache.spark.mllib.regression.LabeledPoint;

import com.knime.bigdata.spark.core.job.SparkClass;
import com.knime.bigdata.spark.node.mllib.prediction.linear.LinearLearnerJobInput;
import com.knime.bigdata.spark1_3.jobs.mllib.prediction.linear.AbstractRegularizationJob;

/**
 * runs MLlib logistic regression on a given RDD to create a classification model
 *
 * @author koetter, dwk
 */
@SparkClass
public class LogisticRegressionJob extends AbstractRegularizationJob<LinearLearnerJobInput> {

    private static final long serialVersionUID = 1L;

    private final static Logger LOGGER = Logger.getLogger(LogisticRegressionJob.class.getName());

    /**
     * {@inheritDoc}
     */
    @Override
    protected Logger getLogger() {
        return LOGGER;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getAlgName() {
        return "Logistic Regression (with SGD or LBFGS)";
    }

    /**
     * Train logistic regression
     *
     * @param aConfig
     * @param aInputData - Training dataset: RDD of LabeledPoint.
     * @return LogisticRegressionModel
     */
    @Override
    public LogisticRegressionModel execute(final SparkContext aContext, final LinearLearnerJobInput aConfig,
        final JavaRDD<LabeledPoint> aInputData) {
        final GeneralizedLinearAlgorithm<LogisticRegressionModel> alg;
        if (aConfig.useSGD()) {
            alg = configureLogRegWithSGD(aConfig);
        } else {
            alg = configureLogRegWithLBFGS(aConfig);
        }

        return alg.run(aInputData.rdd().cache());
    }

    GeneralizedLinearAlgorithm<LogisticRegressionModel> configureLogRegWithSGD(final LinearLearnerJobInput aConfig) {
        final LogisticRegressionWithSGD alg = new LogisticRegressionWithSGD();
        configureSGD(aConfig, alg);
        return alg;
    }

    /**
     * @param alg
     * @param aConfig
     */
    void configureSGD(final LinearLearnerJobInput aConfig, final LogisticRegressionWithSGD alg) {
        configureSGDOptimizer(aConfig, alg.optimizer());
        configureAlgorithm(aConfig, alg);
    }

    /**
     * @param alg
     * @param aConfig
     */
    void configureAlgorithm(final LinearLearnerJobInput aConfig,
        final GeneralizedLinearAlgorithm<LogisticRegressionModel> alg) {
        alg.setFeatureScaling(aConfig.useFeatureScaling()).setIntercept(aConfig.addIntercept())
            .setValidateData(aConfig.validateData());
    }

    GeneralizedLinearAlgorithm<LogisticRegressionModel> configureLogRegWithLBFGS(final LinearLearnerJobInput aConfig) {
        final LogisticRegressionWithLBFGS alg = new LogisticRegressionWithLBFGS();
        alg.optimizer().setNumIterations(aConfig.getNoOfIterations()).setRegParam(aConfig.getRegularization())
            .setUpdater(getUpdater(aConfig)).setGradient(getGradient(aConfig));

        configureAlgorithm(aConfig, alg);
        alg.optimizer().setConvergenceTol(aConfig.getTolerance()).setNumCorrections(aConfig.getNoOfCorrections());

        return alg;
    }
}
