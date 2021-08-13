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
package org.knime.bigdata.spark.node.ml.prediction.linear;

import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.core.port.model.ml.MLMetaData;

/**
 * MLMetaData object that contain metadata about linear regression learner.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class MLLinearLearnerMetaData extends MLMetaData {

    private static final String KEY_COEFFICIENTS = "coefficients";

    private static final String KEY_COEFFICIENTS_STD_ERR = "coefficientStandardErrors";

    private static final String KEY_P_VALUES = "pValues";

    private static final String KEY_T_VALUES = "tValues";

    private static final String KEY_INTERCEPT = "intercept";

    private static final String KEY_R_SQUARED = "rSquared";

    private static final String KEY_R_SQUARED_ADJ = "rSquaredAdjusted";

    private static final String KEY_EXPLAINED_VARIANCE = "explainedVariance";

    private static final String KEY_MEAN_ABS_ERRRO = "meanAbsoluteError";

    private static final String KEY_MEAN_SQUARED_ERROR = "meanSquaredError";

    private static final String KEY_ROOT_MEAN_SQUARED_ERROR = "rootMeanSquaredError";

    /**
     * Constructor for (de)serialization.
     */
    public MLLinearLearnerMetaData() {
    }

    /**
     * Default constructor.
     *
     * @param coefficients coefficients of the model
     * @param intercept intercept of the model
     */
    public MLLinearLearnerMetaData(final double[] coefficients, final double intercept) {
        setDoubleArray(KEY_COEFFICIENTS, coefficients);
        setDouble(KEY_INTERCEPT, intercept);
    }

    /**
     * @param coefficients coefficients of the model
     * @return this meta data data instance
     */
    public MLLinearLearnerMetaData withCoefficients(final double[] coefficients) {
        setDoubleArray(KEY_COEFFICIENTS, coefficients);
        return this;
    }

    /**
     * @return coefficients of the model
     */
    public double[] getCoefficients() {
        return getDoubleArray(KEY_COEFFICIENTS);
    }

    /**
     * @param intercept the intercept
     * @return this meta data data instance
     */
    public MLLinearLearnerMetaData withIntercept(final double intercept) {
        setDouble(KEY_INTERCEPT, intercept);
        return this;
    }

    /**
     * @return intercept of the model
     */
    public double getIntercept() {
        return getDouble(KEY_INTERCEPT);
    }

    /**
     * @param stdErrors coefficients standard errors and intercept in last row if fit intercept enabled
     * @return this meta data data instance
     */
    public MLLinearLearnerMetaData withCoefficientStandardErrors(final double[] stdErrors) {
        setDoubleArray(KEY_COEFFICIENTS_STD_ERR, stdErrors);
        return this;
    }

    /**
     * @return coefficients standard errors and intercept in last row if fit intercept enabled or {@code null} if not
     *         available
     */
    public double[] getCoefficientStandardErrors() {
        return getDoubleArray(KEY_COEFFICIENTS_STD_ERR);
    }

    /**
     * @param values Two-sided p-value of estimated coefficients and intercept in last row if fit intercept enabled
     * @return this meta data data instance
     */
    public MLLinearLearnerMetaData withTValues(final double[] values) {
        setDoubleArray(KEY_T_VALUES, values);
        return this;
    }

    /**
     * @return Two-sided p-value of estimated coefficients and intercept in last row if fit intercept enabled or
     *         {@code null} if not available
     */
    public double[] getTValues() {
        return getDoubleArray(KEY_T_VALUES);
    }

    /**
     * @param values T-statistic of estimated coefficients and intercept in last row if fit intercept enabled
     * @return this meta data data instance
     */
    public MLLinearLearnerMetaData withPValues(final double[] values) {
        setDoubleArray(KEY_P_VALUES, values);
        return this;
    }

    /**
     * @return T-statistic of estimated coefficients and intercept in last row if fit intercept enabled or {@code null}
     *         if not available
     */
    public double[] getPValues() {
        return getDoubleArray(KEY_P_VALUES);
    }

    /**
     * @param rSquared R^2^, the coefficient of determination.
     * @return this meta data data instance
     */
    public MLLinearLearnerMetaData withRSquared(final double rSquared) {
        setDouble(KEY_R_SQUARED, rSquared);
        return this;
    }

    /**
     * @return R^2^, the coefficient of determination.
     */
    public double getRSquared() {
        return getDouble(KEY_R_SQUARED);
    }

    /**
     * @param rSquaredAdj Adjusted R^2^, the adjusted coefficient of determination.
     * @return this meta data data instance
     */
    public MLLinearLearnerMetaData withRSquaredAdjusted(final double rSquaredAdj) {
        setDouble(KEY_R_SQUARED_ADJ, rSquaredAdj);
        return this;
    }

    /**
     * @return Returns Adjusted R^2^, the adjusted coefficient of determination.
     */
    public double getRSquaredAdjusted() {
        return getDouble(KEY_R_SQUARED_ADJ);
    }

    /**
     * @param explainedVariance the explained variance regression score.
     * @return this meta data data instance
     */
    public MLLinearLearnerMetaData withExplainedVariance(final double explainedVariance) {
        setDouble(KEY_EXPLAINED_VARIANCE, explainedVariance);
        return this;
    }

    /**
     * @return The explained variance regression score.
     */
    public double getExplainedVariance() {
        return getDouble(KEY_EXPLAINED_VARIANCE);
    }

    /**
     * @param meanAbsoluteError the mean absolute error, which is a risk function corresponding to the expected value of
     *            the absolute error loss or l1-norm loss.
     * @return this meta data data instance
     */
    public MLLinearLearnerMetaData withMeanAbsoluteError(final double meanAbsoluteError) {
        setDouble(KEY_MEAN_ABS_ERRRO, meanAbsoluteError);
        return this;
    }

    /**
     * @return the mean absolute error, which is a risk function corresponding to the expected value of the absolute
     *         error loss or l1-norm loss.
     */
    public double getMeanAbsoluteError() {
        return getDouble(KEY_MEAN_ABS_ERRRO);
    }

    /**
     * @param meanSquaredError the mean squared error, which is a risk function corresponding to the expected value of
     *            the squared error loss or quadratic loss.
     * @return this meta data data instance
     */
    public MLLinearLearnerMetaData withMeanSquaredError(final double meanSquaredError) {
        setDouble(KEY_MEAN_SQUARED_ERROR, meanSquaredError);
        return this;
    }

    /**
     * @return the mean squared error, which is a risk function corresponding to the expected value of the squared error
     *         loss or quadratic loss.
     */
    public double getMeanSquaredError() {
        return getDouble(KEY_MEAN_SQUARED_ERROR);
    }

    /**
     * @param rootMeanSquaredError the root mean squared error, which is defined as the square root of the mean squared
     *            error.
     * @return this meta data data instance
     */
    public MLLinearLearnerMetaData withRootMeanSquaredError(final double rootMeanSquaredError) {
        setDouble(KEY_ROOT_MEAN_SQUARED_ERROR, rootMeanSquaredError);
        return this;
    }

    /**
     * @return the root mean squared error, which is defined as the square root of the mean squared error.
     */
    public double getRootMeanSquaredError() {
        return getDouble(KEY_ROOT_MEAN_SQUARED_ERROR);
    }

}
