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
package org.knime.bigdata.spark3_1.jobs.ml.prediction.linear.classification;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
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
import org.knime.bigdata.spark3_1.api.MLUtils;
import org.knime.bigdata.spark3_1.jobs.ml.prediction.MLClassificationLearnerJob;

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
    protected Classifier<?, ?, ?> createClassifier(final MLLogisticRegressionLearnerJobInput input) {
        final LogisticRegression logisticRegression = new LogisticRegression() //
            .setFamily(input.getFamily()) //
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

        final Matrix coeffMatr = lrModel.coefficientMatrix();
        final List<String> featureValues = MLUtils.getNominalFeatureValuesAddTargetValuesToMetaData(pipelineModel, metaData);


        if (isMultinomial) {
            final ArrayList<String> targetLabels = new ArrayList<>();
            final ArrayList<String> variableLabels = new ArrayList<>();
            final ArrayList<Double> coefficients = new ArrayList<>();

            final List<String> targetValues = metaData.getNominalTargetValueMappings();
            final double[] intercept = lrModel.interceptVector().toArray();

            int targetIndex = 0;
            final Iterator<Vector> rowIter = coeffMatr.rowIter();
            while(rowIter.hasNext()) {
                final double[] coeffRow = rowIter.next().toArray();
                for (int i = 0; i < coeffRow.length; i++) {
                    targetLabels.add(targetValues.get(targetIndex));
                    variableLabels.add(featureValues.get(i));
                    coefficients.add(coeffRow[i]);
                }
                if (lrModel.getFitIntercept()) {
                    targetLabels.add(targetValues.get(targetIndex));
                    variableLabels.add("Intercept");
                    coefficients.add(intercept[targetIndex]);
                }
                targetIndex++;
            }

            final LogisticRegressionTrainingSummary summary = lrModel.summary();
            final ArrayList<double[]> accuracyStatRows = new ArrayList<>();
            final double[] falsePositiveRate = summary.falsePositiveRateByLabel();
            final double[] truePositiveRate = summary.truePositiveRateByLabel();
            final double[] recall = summary.recallByLabel();
            final double[] precission = summary.precisionByLabel();
            final double[] fMeasure = summary.fMeasureByLabel();

            for (int i = 0; i < falsePositiveRate.length; i++) {
                accuracyStatRows.add(new double[] { falsePositiveRate[i], truePositiveRate[i], recall[i], precission[i], fMeasure[i] });
            }

            accuracyStatRows.add(new double[] {
                summary.weightedFalsePositiveRate(), //
                summary.weightedTruePositiveRate(), //
                summary.weightedRecall(), //
                summary.weightedPrecision(), //
                summary.weightedFMeasure() //
            });

            return metaData.withCoefficients(targetLabels, variableLabels, coefficients).withAccuracy(accuracyStatRows, summary.accuracy());

        } else {
            final ArrayList<String> targetLabels = new ArrayList<>();
            final ArrayList<String> variableLabels = new ArrayList<>();
            final ArrayList<Double> coefficients = new ArrayList<>();

            variableLabels.addAll(featureValues);
            for (final double coeff : lrModel.coefficients().toArray()) {
                coefficients.add(coeff);
            }

            if (lrModel.getFitIntercept()) {
                variableLabels.add("Intercept");
                coefficients.add(lrModel.intercept());
            }

            final BinaryLogisticRegressionTrainingSummary summary = lrModel.binarySummary();
            final ArrayList<double[]> accuracyStatRows = new ArrayList<>();

            accuracyStatRows.add(new double[] {
                summary.weightedFalsePositiveRate(), //
                summary.weightedTruePositiveRate(), //
                summary.weightedRecall(), //
                summary.weightedPrecision(), //
                summary.weightedFMeasure() //
            });

            return metaData.withCoefficients(targetLabels, variableLabels, coefficients).withAccuracy(accuracyStatRows, summary.accuracy());
        }
    }

    @Override
    protected boolean useNominalDummyVariables() {
        return true;
    }

    @Override
    protected String getTargetStringOrderType() {
        return "alphabetAsc";
    }

    @Override
    protected String getNominalFeatureStringOrderType() {
        return "alphabetAsc";
    }

}
