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
 *   Created on Oct 1, 2015 by bjoern
 */
package com.knime.bigdata.spark.node.scorer.numeric;

/**
 * Holds the results of numeric scoring that are displayed in {@link SparkNumericScorerNodeView}.
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class SparkNumericScorerViewData {

    private final double m_rSquare;

    private final double m_meanAbsError;

    private final double m_meanSquaredError;

    private final double m_rootMeanSquaredDeviation;

    private final double m_meanSignedDifference;

    /**
     * @param rSquare
     * @param meanAbsError
     * @param meanSquaredError
     * @param rootMeanSquaredDeviation
     * @param meanSignedDifference
     */
    public SparkNumericScorerViewData(final double rSquare, final double meanAbsError, final double meanSquaredError,
        final double rootMeanSquaredDeviation, final double meanSignedDifference) {
        this.m_rSquare = rSquare;
        this.m_meanAbsError = meanAbsError;
        this.m_meanSquaredError = meanSquaredError;
        this.m_rootMeanSquaredDeviation = rootMeanSquaredDeviation;
        this.m_meanSignedDifference = meanSignedDifference;
    }

    /**
     * @return the RSquare
     */
    public double getRSquare() {
        return m_rSquare;
    }

    /**
     * @return the meanAbsError
     */
    public double getMeanAbsError() {
        return m_meanAbsError;
    }

    /**
     * @return the meanSquaredError
     */
    public double getMeanSquaredError() {
        return m_meanSquaredError;
    }

    /**
     * @return the root mean squared deviation
     */
    public double getRootMeanSquaredDeviation() {
        return m_rootMeanSquaredDeviation;
    }

    /**
     * @return the meanSignedDifference
     */
    public double getMeanSignedDifference() {
        return m_meanSignedDifference;
    }
}
