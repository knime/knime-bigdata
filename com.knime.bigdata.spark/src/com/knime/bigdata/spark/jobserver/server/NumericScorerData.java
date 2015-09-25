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
 *   Created on 22.09.2015 by dwk
 */
package com.knime.bigdata.spark.jobserver.server;

import java.io.Serializable;

/**
 *
 * @author dwk
 */
public class NumericScorerData implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    /**
     * Number of rows in the input table. Interesting if you want to know the number of missing values in either of the
     * target columns.
     */
    private final long m_nrRows;

    private final double m_rSquared;

    private final double m_meanAbsError;

    private final double m_meanSquaredError;

    private final double m_rmsd;

    private final double m_meanSignedDifference;

    private final Integer m_ClassCol;

    private final Integer m_PredictionCol;

    /**
     * default constructor
     * @param aNrRows
     * @param aRSquared
     * @param aAbsError
     * @param aSquaredError
     * @param aRootMeanSquaredDeviation
     * @param aSignedDiff
     * @param aClassCol
     * @param aPredictionCol
     */
    public NumericScorerData(final long aNrRows, final double aRSquared, final double aAbsError,
        final double aSquaredError, final double aRootMeanSquaredDeviation, final double aSignedDiff,
        final Integer aClassCol, final Integer aPredictionCol) {
        m_nrRows = aNrRows;

        m_rSquared = aRSquared;

        m_meanAbsError = aAbsError;

        m_meanSquaredError = aSquaredError;

        m_rmsd = aRootMeanSquaredDeviation;

        m_meanSignedDifference = aSignedDiff;

        m_ClassCol = aClassCol;

        m_PredictionCol = aPredictionCol;
    }

    /**
     * @return the R^2 value
     */
    public double getRSquare() {
        return m_rSquared;
    }

    /**
     * @return the mean absolute error
     */
    public double getMeanAbsError() {
        return m_meanAbsError;
    }

    /**
     * @return the mean squared error
     */
    public double getMeanSquaredError() {
        return m_meanSquaredError;
    }

    /**
     * @return the root mean squared deviation
     */
    public double getRmsd() {
        return m_rmsd;
    }

    /**
     * @return the mean signed difference
     */
    public double getMeanSignedDifference() {
        return m_meanSignedDifference;
    }

    /**
     *
     * @return number of rows in scored data set
     */
    public long getNrRows() {
        return m_nrRows;
    }

    /**
     *
     * @return index of column with original values
     */
    public int getFirstCompareColumn() {
        return m_ClassCol;
    }

    /**
     *
     * @return index of column with predicted values
     */
    public int getSecondCompareColumn() {
        return m_PredictionCol;
    }
}
