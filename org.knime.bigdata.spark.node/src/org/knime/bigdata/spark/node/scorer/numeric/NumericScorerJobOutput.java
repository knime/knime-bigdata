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
 *
 * History
 *   Created on May 13, 2016 by oole
 */
package org.knime.bigdata.spark.node.scorer.numeric;

import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.scorer.ScorerJobOutput;

/**
 *
 * @author Ole Ostergaard
 */
@SparkClass
public class NumericScorerJobOutput extends ScorerJobOutput {

    private static final String R2 = "r2";
    private static final String ABS_ERROR = "absError";
    private static final String SQUARED_ERROR = "squaredError";
    private static final String ROOT_SQUARED_ERROR = "rootSquaredError";
    private static final String SIGNED_DIFF = "signedDiff";

    /**
     * Paramless constructor for autmatic deserialization.
     */
    public NumericScorerJobOutput() {}

    /**
     * @param r2
     * @param absError
     * @param squaredError
     * @param rootSquaredError
     * @param signedDiff
     * @param missingValueRowCount count of rows with missing values
     */
    public NumericScorerJobOutput(final double r2, final double absError, final double squaredError, final double rootSquaredError,
        final double signedDiff, final long missingValueRowCount) {
        super(missingValueRowCount);
        set(R2, r2);
        set(ABS_ERROR, absError);
        set(SQUARED_ERROR, squaredError);
        set(ROOT_SQUARED_ERROR, rootSquaredError);
        set(SIGNED_DIFF, signedDiff);
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
}
