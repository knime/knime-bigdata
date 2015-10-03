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
import java.util.List;

/**
 *
 * @author dwk
 */
public class ScorerData implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    /**
     * The confusion matrix as int 2-D array.
     */
    private final int[][] m_scorerCount;

    /**
     * Number of rows in the input table. Interesting if you want to know the number of missing values in either of the
     * target columns.
     */
    private final long m_nrRows;

    /** Number of misclassifications. */
    private final int m_falseCount;

    /** Number of correct classifications. */
    private final int m_correctCount;

    /**
     * The index of the first column (usually containing the real values).
     */
    private final int m_firstCompareColumn;

    /**
     * The index of the second column (usually containing the predicted values).
     */
    private final int m_secondCompareColumn;

    /**
     * All possible target values.
     */
    private final Object[] m_targetValues;

    /**
     * default constructor
     * @param scorerCount
     * @param nrRows
     * @param falseCount
     * @param correctCount
     * @param firstCompareColumn
     * @param secondCompareColumn
     * @param targetValues
     */
    public ScorerData(final int[][] scorerCount, final long nrRows, final int falseCount, final int correctCount,
        final int firstCompareColumn, final int secondCompareColumn, final List<Object> targetValues) {
        m_scorerCount = scorerCount;
        m_nrRows = nrRows;
        m_falseCount = falseCount;
        m_correctCount = correctCount;
        m_firstCompareColumn = firstCompareColumn;
        m_secondCompareColumn = secondCompareColumn;
        m_targetValues = targetValues.toArray();
    }

    /**
     *
     * @return index of column with original values
     */
    public int getFirstCompareColumn() {
        return m_firstCompareColumn;
    }

    /**
     *
     * @return index of column with predicted values
     */
    public int getSecondCompareColumn() {
        return m_secondCompareColumn;
    }

    /**
     *
     * @return Number of correct classifications.
     */
    public int getCorrectCount() {
        return m_correctCount;
    }

    /**
     * @return Number of misclassifications.
     */
    public int getFalseCount() {
        return m_falseCount;
    }

    /**
     *
     * @return All possible target values.
     */
    public Object[] getTargetValues() {
        return m_targetValues;
    }

    /**
     *
     * @return The confusion matrix as int 2-D array.
     */
    public int[][] getScorerCount() {
        return m_scorerCount;
    }

    /**
     *
     * @return Number of rows in the input table
     */
    public long getNrRows() {
        return m_nrRows;
    }
}
