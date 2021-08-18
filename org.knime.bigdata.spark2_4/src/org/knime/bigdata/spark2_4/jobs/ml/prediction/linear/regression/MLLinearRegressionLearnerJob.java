/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
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
 */
package org.knime.bigdata.spark2_4.jobs.ml.prediction.linear.regression;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.spark.SparkContext;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.Predictor;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionSummary;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.port.model.ml.MLMetaData;
import org.knime.bigdata.spark.node.ml.prediction.linear.regression.MLLinearRegressionLearnerJobInput;
import org.knime.bigdata.spark.node.ml.prediction.linear.regression.MLLinearRegressionLearnerMetaData;
import org.knime.bigdata.spark2_4.api.MLUtils;
import org.knime.bigdata.spark2_4.jobs.ml.prediction.MLRegressionLearnerJob;

/**
 * Learner job for spark.ml-based linear regression models.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class MLLinearRegressionLearnerJob extends MLRegressionLearnerJob<MLLinearRegressionLearnerJobInput> {

    private static final long serialVersionUID = 2157035134300354540L;

    @Override
    protected Predictor<?, ?, ?> createRegressor(final MLLinearRegressionLearnerJobInput input)
        throws KNIMESparkException {

        final LinearRegression linearRegression = new LinearRegression() //
            .setLoss(input.getLossFunction()) //
            .setMaxIter(input.getMaxIter()) //
            .setStandardization(input.getStandardization()) //
            .setFitIntercept(input.getFitIntercept()) //
            .setSolver(input.getSolver()) //
            .setTol(input.getConvergenceTolerance());

        switch (input.getRegularizer()) {
            case "NONE":
                linearRegression.setRegParam(0);
                break;

            case "RIDGE":
                linearRegression.setRegParam(input.getRegParam());
                linearRegression.setElasticNetParam(0);
                break;

            case "LASSO":
                linearRegression.setRegParam(input.getRegParam());
                linearRegression.setElasticNetParam(1);
                break;

            case "ELASTIC_NET":
                linearRegression.setRegParam(input.getRegParam());
                linearRegression.setElasticNetParam(input.getElasticNetParam());
                break;

            default:
                throw new KNIMESparkException("Unknown regularizer");
        }

        return linearRegression;
    }

    @Override
    protected Path generateModelInterpreterData(final SparkContext sparkContext, final PipelineModel pipelineModel)
        throws IOException {
        return null; // no additional interpreter data available
    }

    @Override
    protected MLMetaData createModelMetaData(final PipelineModel pipelineModel) {
        final LinearRegressionModel lrModel = MLUtils.findFirstStageOfType(pipelineModel, LinearRegressionModel.class);
        final MLLinearRegressionLearnerMetaData metaData = new MLLinearRegressionLearnerMetaData() //
                .withCoefficients(lrModel.coefficients().toArray()) //
                .withIntercept(lrModel.intercept());

        if (lrModel.hasSummary()) {
            final LinearRegressionSummary summary = lrModel.summary();
            metaData //
                .withRSquared(summary.r2()) //
                .withRSquaredAdjusted(summary.r2adj()) //
                .withExplainedVariance(summary.explainedVariance()) //
                .withMeanAbsoluteError(summary.meanAbsoluteError()) //
                .withMeanSquaredError(summary.meanSquaredError()) //
                .withRootMeanSquaredError(summary.rootMeanSquaredError());

            try {
                metaData //
                    .withCoefficientStandardErrors(summary.coefficientStandardErrors()) //
                    .withTValues(summary.tValues()) //
                    .withPValues(summary.pValues());
            } catch (final UnsupportedOperationException e) { // NOSONAR values might be not available
            }
        }

        MLUtils.addNominalFeatureValuesMappingsToMetaData(pipelineModel, metaData);

        return metaData;
    }

    @Override
    protected boolean useNominalDummyVariables() {
        return true;
    }

    @Override
    protected String getNominalFeatureStringOrderType() {
        return "alphabetAsc";
    }

}
