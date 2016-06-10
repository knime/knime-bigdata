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
 *   Created on May 13, 2016 by oole
 */
package com.knime.bigdata.spark.node.scorer.numeric;

import com.knime.bigdata.spark.core.job.JobOutput;
import com.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Ole Ostergaard
 */
@SparkClass
public class NumericScorerJobOutput extends JobOutput {

    private static final String ROW_COUNT = "rowCount";
    private static final String R2 = "r2";
    private static final String ABS_ERROR = "absError";
    private static final String SQUARED_ERROR = "squaredError";
    private static final String ROOT_SQUARED_ERROR = "rootSquaredError";
    private static final String SIGNED_DIFF = "signedDiff";
    private static final String CLASS_COL = "classCol";
    private static final String PREDICTION_COL = "predictionCol";

    /**
     * Paramless constructor for autmatic deserialization.
     */
    public NumericScorerJobOutput() {}

    /**
     * @param rowCount
     * @param r2
     * @param absError
     * @param squaredError
     * @param rootSquaredError
     * @param signedDiff
     * @param classCol
     * @param predictionCol
     */
    public NumericScorerJobOutput(final long rowCount, final double r2, final double absError, final double squaredError, final double rootSquaredError,
        final double signedDiff, final Integer classCol, final Integer predictionCol) {
        set(ROW_COUNT, rowCount);
        set(R2, r2);
        set(ABS_ERROR, absError);
        set(SQUARED_ERROR, squaredError);
        set(ROOT_SQUARED_ERROR, rootSquaredError);
        set(SIGNED_DIFF, signedDiff);
        set(CLASS_COL, classCol);
        set(PREDICTION_COL, predictionCol);
    }

    /**
     * @return the row count
     */
    public long getRowCount() {
        return getLong(ROW_COUNT);
    }

    /**
     * @return R^2
     */
    public double getR2() {
        return getDouble(R2);
    }

    /**
     * @return the mean absolute error
     */
    public double getAbsError() {
        return getDouble(ABS_ERROR);
    }

    /**
     * @return the mean squared error
     */
    public double getSquaredError() {
        return getDouble(SQUARED_ERROR);
    }

    /**
     * @return the root mean squared deviation
     */
    public double getRootSquaredError() {
        return getDouble(ROOT_SQUARED_ERROR);
    }

    /**
     * @return the mean signed difference
     */
    public double getSignedDiff() {
        return getDouble(SIGNED_DIFF);
    }

    /**
     * @return the index of the class column
     */
    public Integer getClassCol() {
        return getInteger(CLASS_COL);
    }

    /**
     * @return the index of the prediction column
     */
    public Integer getPredictionCol() {
        return getInteger(PREDICTION_COL);
    }

}
