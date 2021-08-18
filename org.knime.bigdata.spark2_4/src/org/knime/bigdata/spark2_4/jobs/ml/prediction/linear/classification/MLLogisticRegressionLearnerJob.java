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
package org.knime.bigdata.spark2_4.jobs.ml.prediction.linear.classification;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.classification.BinaryLogisticRegressionTrainingSummary;
import org.apache.spark.ml.classification.Classifier;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.classification.LogisticRegressionTrainingSummary;
import org.apache.spark.ml.linalg.Matrix;
import org.apache.spark.ml.linalg.Vector;
import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.port.model.ml.MLMetaData;
import org.knime.bigdata.spark.node.ml.prediction.linear.classification.MLLogisticRegressionLearnerJobInput;
import org.knime.bigdata.spark.node.ml.prediction.linear.classification.MLLogisticRegressionLearnerMetaData;
import org.knime.bigdata.spark2_4.api.MLUtils;
import org.knime.bigdata.spark2_4.jobs.ml.prediction.MLClassificationLearnerJob;

import scala.collection.Iterator;

/**
 * Learner job for spark.ml-based logistic regression models.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class MLLogisticRegressionLearnerJob extends MLClassificationLearnerJob<MLLogisticRegressionLearnerJobInput> {

    private static final long serialVersionUID = 2157035134300354540L;

    @Override
    protected boolean useNominalDummyVariables() {
        return true;
    }

    @Override
    protected Classifier<?, ?, ?> createClassifier(final MLLogisticRegressionLearnerJobInput input) {
        final LogisticRegression logisticRegression = new LogisticRegression() //
            .setFamily("auto") //
            .setMaxIter(input.getMaxIter()) //
            .setStandardization(input.getStandardization()) //
            .setFitIntercept(input.getFitIntercept()) //
            .setTol(input.getConvergenceTolerance());

        switch (input.getRegularizer()) {
            case "NONE":
                logisticRegression.setRegParam(0);
                break;

            case "RIDGE":
                logisticRegression.setRegParam(input.getRegParam());
                logisticRegression.setElasticNetParam(0);
                break;

            case "LASSO":
                logisticRegression.setRegParam(input.getRegParam());
                logisticRegression.setElasticNetParam(1);
                break;

            case "ELASTIC_NET":
                logisticRegression.setRegParam(input.getRegParam());
                logisticRegression.setElasticNetParam(input.getElasticNetParam());
                break;

            default:
                throw new RuntimeException("Unknown regularizer");
        }

        return logisticRegression;
    }

    @Override
    protected Path generateModelInterpreterData(final SparkContext sparkContext, final PipelineModel pipelineModel)
        throws IOException {
        // MLMetaData contains coefficients + intercept, no additional interpreter data available
        return null;
    }

    @Override
    protected MLMetaData createModelMetaData(final PipelineModel pipelineModel) {
        final LogisticRegressionModel lrModel = MLUtils.findFirstStageOfType(pipelineModel, LogisticRegressionModel.class);
        final boolean isMultinomial = lrModel.org$apache$spark$ml$classification$LogisticRegressionModel$$isMultinomial();
        final MLLogisticRegressionLearnerMetaData metaData = new MLLogisticRegressionLearnerMetaData(isMultinomial);

        final List<String> featureValues = MLUtils.getNominalFeatureValuesAddTargetValuesToMetaData(pipelineModel, metaData);
        final ArrayList<String> colNames = new ArrayList<>();
        colNames.addAll(featureValues);

        if (isMultinomial) {
            final ArrayList<ArrayList<Double>> rows = new ArrayList<>();
            final Matrix coeffMatr = lrModel.coefficientMatrix();
            final double[] intercept;
            if (lrModel.getFitIntercept()) {
                intercept = lrModel.interceptVector().toArray();
                colNames.add("Intercept");
            } else {
                intercept = new double[0];
            }

            final LogisticRegressionTrainingSummary summary = lrModel.summary();
            final double[] falsePositiveRate = summary.falsePositiveRateByLabel();
            colNames.add("False Positive Rate");
            final double[] truePositiveRate = summary.truePositiveRateByLabel();
            colNames.add("True Positive Rate");
            final double[] precission = summary.precisionByLabel();
            colNames.add("Precission");
            final double[] recall = summary.recallByLabel();
            colNames.add("Recall");
            final double[] fMeasure = summary.fMeasureByLabel();
            colNames.add("F-measure");

            final Iterator<Vector> rowIter = coeffMatr.rowIter();
            int i = 0;
            while(rowIter.hasNext()) {
                final double[] coeffRow = rowIter.next().toArray();
                final ArrayList<Double> row = new ArrayList<>(coeffRow.length + 6);
                for (double coeff : coeffRow) {
                    row.add(coeff);
                }

                if (lrModel.getFitIntercept()) {
                    row.add(intercept[i]);
                }

                row.add(falsePositiveRate[i]);
                row.add(truePositiveRate[i]);
                row.add(precission[i]);
                row.add(recall[i]);
                row.add(fMeasure[i]);

                rows.add(row);
            }

            return metaData.withCoefficients(colNames, rows) //
                    .withAccuracy(summary.accuracy()) //
                    .withWeightedFalsePositiveRate(summary.weightedFalsePositiveRate()) //
                    .withWeightedTruePositiveRate(summary.weightedTruePositiveRate()) //
                    .withWeightedFMeasure(summary.weightedFMeasure()) //
                    .withWeightedPrecission(summary.weightedPrecision()) //
                    .withWeightedRecall(summary.weightedRecall());

        } else {
            final BinaryLogisticRegressionTrainingSummary summary = lrModel.binarySummary();
            final double[] coeffRow = lrModel.coefficients().toArray();
            final ArrayList<Double> row = new ArrayList<>(coeffRow.length + 1);

            for (double coeff : coeffRow) {
                row.add(coeff);
            }

            if (lrModel.getFitIntercept()) {
                row.add(lrModel.intercept());
                colNames.add("Intercept");
            }

            return metaData.withCoefficients(colNames, Collections.singletonList(row)) //
                    .withAccuracy(summary.accuracy()) //
                    .withWeightedFalsePositiveRate(summary.weightedFalsePositiveRate()) //
                    .withWeightedTruePositiveRate(summary.weightedTruePositiveRate()) //
                    .withWeightedFMeasure(summary.weightedFMeasure()) //
                    .withWeightedPrecission(summary.weightedPrecision()) //
                    .withWeightedRecall(summary.weightedRecall());
        }
    }
}
