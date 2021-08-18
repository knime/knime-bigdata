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
package org.knime.bigdata.spark.node.ml.prediction.linear.classification;

import java.util.ArrayList;
import java.util.List;

import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.port.model.ml.MLMetaData;

/**
 * MLMetaData object that contain metadata about linear regression learner.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class MLLogisticRegressionLearnerMetaData extends MLMetaData {

    private static final String KEY_MULTINOMIAL = "multinomial";

    private static final String KEY_COEFF_COL_LABLES = "coefficientColLabels";

    private static final String KEY_COEFF_ROWS = "coefficientRows";

    private static final String KEY_ACCURACY = "accuracy";

    private static final String KEY_WEIGHTED_FALSE_POSITIVE_RATE = "weightedFalsePositiveRate";

    private static final String KEY_WEIGHTED_TRUE_POSITIVE_RATE = "weightedTruePositiveRate";

    private static final String KEY_WEIGHTED_F_MEASURE = "weightedFMeasure";

    private static final String KEY_WEIGHTED_PRECISION = "weightedPrecision";

    private static final String KEY_WEIGHTED_RECALL = "weightedRecall";

    /**
     * Constructor for (de)serialization.
     */
    public MLLogisticRegressionLearnerMetaData() {
    }

    /**
     * Default constructor.
     * @param isMultinominal {@code false} if binomial
     */
    public MLLogisticRegressionLearnerMetaData(final boolean isMultinominal) {
        set(KEY_MULTINOMIAL, isMultinominal);
    }

    /**
     * @return {@code false} if binomial, {@code true} if multinomial
     */
    public boolean isMultinominal() {
        return get(KEY_MULTINOMIAL);
    }

    /**
     * Set coefficient column labels and rows
     * @param coeffColLabels labels of the columns
     * @param coeffRows rows of the coefficient table
     * @return this meta data data instance
     */
    public MLLogisticRegressionLearnerMetaData withCoefficients(final List<String> coeffColLabels, final List<ArrayList<Double>> coeffRows) {
        set(KEY_COEFF_COL_LABLES, coeffColLabels);
        set(KEY_COEFF_ROWS, coeffRows);
        return this;
    }

    /**
     * @return labels of the coefficient table columns
     */
    public List<String> getCoefficientCols() {
        return get(KEY_COEFF_COL_LABLES);
    }

    /**
     * @return rows of the coefficient table
     */
    public List<List<Double>> getCoefficientRows() {
        return get(KEY_COEFF_ROWS);
    }

    /**
     * @param accuracy
     * @return this meta data data instance
     */
    public MLLogisticRegressionLearnerMetaData withAccuracy(final double accuracy) {
        setDouble(KEY_ACCURACY, accuracy);
        return this;
    }

    /**
     * @return accuracy, equals to the total number of correctly classified instances out of the total number of
     *         instances.
     */
    public double getAccuracy() {
        return getDouble(KEY_ACCURACY);
    }

    /**
     * @param weightedFalsePositiveRate
     * @return this meta data data instance
     */
    public MLLogisticRegressionLearnerMetaData withWeightedFalsePositiveRate(final double weightedFalsePositiveRate) {
        setDouble(KEY_WEIGHTED_FALSE_POSITIVE_RATE, weightedFalsePositiveRate);
        return this;
    }

    /**
     * @return weighted false positive rate.
     */
    public double getWeightedFalsePositiveRate() {
        return getDouble(KEY_WEIGHTED_FALSE_POSITIVE_RATE);
    }

    /**
     * @param weightedTruePositiveRate
     * @return this meta data data instance
     */
    public MLLogisticRegressionLearnerMetaData withWeightedTruePositiveRate(final double weightedTruePositiveRate) {
        setDouble(KEY_WEIGHTED_TRUE_POSITIVE_RATE, weightedTruePositiveRate);
        return this;
    }

    /**
     * @return weighted true positive rate. (equals to precision, recall and f-measure)
     */
    public double getWeightedTruePositiveRate() {
        return getDouble(KEY_WEIGHTED_TRUE_POSITIVE_RATE);
    }

    /**
     * @param weightedFMeasure
     * @return this meta data data instance
     */
    public MLLogisticRegressionLearnerMetaData withWeightedFMeasure(final double weightedFMeasure) {
        setDouble(KEY_WEIGHTED_F_MEASURE, weightedFMeasure);
        return this;
    }

    /**
     * @return weighted averaged f1-measure.
     */
    public double getWeightedFMeasure() {
        return getDouble(KEY_WEIGHTED_F_MEASURE);
    }

    /**
     * @param weightedPrecision
     * @return this meta data data instance
     */
    public MLLogisticRegressionLearnerMetaData withWeightedPrecission(final double weightedPrecision) {
        setDouble(KEY_WEIGHTED_PRECISION, weightedPrecision);
        return this;
    }

    /**
     * @return weighted averaged precision
     */
    public double getWeightedPrecission() {
        return getDouble(KEY_WEIGHTED_PRECISION);
    }

    /**
     * @param weightedRecall
     * @return this meta data data instance
     */
    public MLLogisticRegressionLearnerMetaData withWeightedRecall(final double weightedRecall) {
        setDouble(KEY_WEIGHTED_RECALL, weightedRecall);
        return this;
    }

    /**
     * @return weighted averaged recall. (equals to precision, recall and f-measure)
     */
    public double getWeightedRecall() {
        return getDouble(KEY_WEIGHTED_RECALL);
    }

}
