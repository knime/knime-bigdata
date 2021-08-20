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

    private static final String KEY_COEFF_TARGETS = "coefficientLogit";

    private static final String KEY_COEFF_VARIABLES = "coefficientVariable";

    private static final String KEY_COEFF = "coefficients";

    private static final String KEY_ACCURACY_STAT_ROWS = "accuracyStatisticRows";

    private static final String KEY_ACCURACY = "accuracy";


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
     * Set coefficient target, variables and coefficients
     *
     * @param targetsLabels target column labels
     * @param variableLabels variable column labels
     * @param coefficients coefficients column values
     *
     * @return this meta data data instance
     */
    public MLLogisticRegressionLearnerMetaData withCoefficients(final List<String> targetsLabels, final List<String> variableLabels, final List<Double> coefficients) {
        set(KEY_COEFF_TARGETS, targetsLabels);
        set(KEY_COEFF_VARIABLES, variableLabels);
        set(KEY_COEFF, coefficients);
        return this;
    }

    public List<String> coeffTagetLabels() {
        return get(KEY_COEFF_TARGETS);
    }

    public List<String> coeffVariableLabels() {
        return get(KEY_COEFF_VARIABLES);
    }

    public List<Double> coefficients() {
        return get(KEY_COEFF);
    }

    /**
     * Set accuracy statistics
     *
     * @param accuracyStatRows accuracy rows for each target class and one weighted row
     * @param accuracy the total number of correctly classified instances out of the total number of instances.
     *
     * @return this meta data data instance
     */
    public MLLogisticRegressionLearnerMetaData withAccuracy(final List<double[]> accuracyStatRows, final double accuracy) {
        set(KEY_ACCURACY_STAT_ROWS, accuracyStatRows);
        setDouble(KEY_ACCURACY, accuracy);
        return this;
    }

    /**
     * @return accuracy statistic rows
     */
    public List<double[]> getAccuracyStatRows() {
        return get(KEY_ACCURACY_STAT_ROWS);
    }

    /**
     * @return accuracy, equals to the total number of correctly classified instances out of the total number of
     *         instances.
     */
    public double getAccuracy() {
        return getDouble(KEY_ACCURACY);
    }

}
