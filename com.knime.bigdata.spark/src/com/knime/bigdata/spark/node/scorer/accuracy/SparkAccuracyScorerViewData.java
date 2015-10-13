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
 *   Created on 30.09.2015 by Bjoern Lohrmann
 */
package com.knime.bigdata.spark.node.scorer.accuracy;

/**
 * Class that holds the view information for the scorer.
 *
 * TODO: Remove class, because duplicate of org.knime.base.node.mine.scorer.accuracy.ScorerViewData with hilite functionality removed.
 *
 * @author Thorsten Meinl, KNIME.com, Zurich, Switzerland
 */
final class SparkAccuracyScorerViewData {
    /**
     * The confusion matrix as int 2-D array.
     */
    private final int[][] m_scorerCount;

    /**
     * Number of rows in the input table. Interesting if you want to know the number of missing values in either of the
     * target columns.
     */
    private final int m_nrRows;

    /** Number of misclassifications. */
    private final int m_falseCount;

    /** Number of correct classifications. */
    private final int m_correctCount;

    /**
     * The first column (usually containing the real values).
     */
    private final String m_firstCompareColumn;

    /**
     * The second column (usually containing the predicted values).
     */
    private final String m_secondCompareColumn;

    /**
     * All possible target values.
     */
    private final String[] m_targetValues;

    SparkAccuracyScorerViewData(final int[][] scorerCount, final int nrRows, final int falseCount, final int correctCount,
        final String firstCompareColumn, final String secondCompareColumn, final String[] targetValues) {
        m_scorerCount = scorerCount;
        m_nrRows = nrRows;
        m_falseCount = falseCount;
        m_correctCount = correctCount;
        m_firstCompareColumn = firstCompareColumn;
        m_secondCompareColumn = secondCompareColumn;
        m_targetValues = targetValues;
    }

    String getFirstCompareColumn() {
        return m_firstCompareColumn;
    }


    String getSecondCompareColumn() {
        return m_secondCompareColumn;
    }

    int getCorrectCount() {
        return m_correctCount;
    }

    int getFalseCount() {
        return m_falseCount;
    }

    String[] getTargetValues() {
        return m_targetValues;
    }

    int[][] getScorerCount() {
        return m_scorerCount;
    }

    int getNrRows() {
        return m_nrRows;
    }

    /**
     * @return ratio of correct classified and all patterns
     */
    double getAccuracy() {
        double totalNumberDataSets = m_falseCount + m_correctCount;
        if (totalNumberDataSets == 0) {
            return Double.NaN;
        } else {
            return m_correctCount / totalNumberDataSets;
        }
    }

    /**
     * @return ratio of wrong classified and all patterns
     */
    double getError() {
        double totalNumberDataSets = m_falseCount + m_correctCount;
        if (totalNumberDataSets == 0) {
            return Double.NaN;
        } else {
            return m_falseCount / totalNumberDataSets;
        }
    }

    /**
     * @return Cohen's Kappa
     * @since 2.9
     */
    double getCohenKappa() {
        long[] rowSum = new long[m_scorerCount[0].length];
        long[] colSum = new long[m_scorerCount.length];
        //Based on: https://en.wikipedia.org/wiki/Cohen%27s_kappa#
        long agreement = 0, sum = 0;
        for (int i = 0; i < rowSum.length; i++) {
            for (int j = 0; j < colSum.length; j++) {
                rowSum[i] += m_scorerCount[i][j];
                colSum[j] += m_scorerCount[i][j];
                sum += m_scorerCount[i][j];
            }
            //number of correct agreements
            agreement += m_scorerCount[i][i];
        }
        //relative observed agreement among raters
        final double p0 = agreement * 1d / sum;
        //hypothetical probability of chance agreement
        double pe = 0;
        for (int i = 0; i < m_scorerCount.length; i++) {
            //Expected value that they agree by chance for possible value i
            pe += 1d * rowSum[i] * colSum[i] / sum / sum;
        }
        //kappa
        return (p0 - pe) / (1 - pe);
    }

    int getTP(final int classIndex) {
        return m_scorerCount[classIndex][classIndex];
    }

    int getFN(final int classIndex) {
        int ret = 0;
        for (int i = 0; i < m_scorerCount[classIndex].length; i++) {
            if (classIndex != i) {
                ret += m_scorerCount[classIndex][i];
            }
        }
        return ret;
    }

    int getTN(final int classIndex) {
        int ret = 0;
        for (int i = 0; i < m_scorerCount.length; i++) {
            if (i != classIndex) {
                for (int j = 0; j < m_scorerCount[i].length; j++) {
                    if (classIndex != j) {
                        ret += m_scorerCount[i][j];
                    }
                }
            }
        }
        return ret;
    }

    int getFP(final int classIndex) {
        int ret = 0;
        for (int i = 0; i < m_scorerCount.length; i++) {
            if (classIndex != i) {
                ret += m_scorerCount[i][classIndex];
            }
        }
        return ret;
    }
}
