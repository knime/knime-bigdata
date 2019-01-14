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
package org.knime.bigdata.spark.node.scorer.accuracy;

import java.util.List;

import org.knime.bigdata.spark.core.job.SparkClass;
import org.knime.bigdata.spark.node.scorer.ScorerJobOutput;

/**
 *
 * @author Ole Ostergaard
 */
@SparkClass
public class AccuracyScorerJobOutput extends ScorerJobOutput {

    private static final String CONFUSION_MATRIX = "confusionMatrix";
    private static final String ROW_COUNT = "rowCount";
    private static final String FALSE_COUNT = "falseCount";
    private static final String CORRECT_COUNT = "correctCount";
    private static final String LABELS = "labels";


    /**
     * Paramless constructor for automatic deserialization.
     */
    public AccuracyScorerJobOutput() {}

    /**
     * @param confusionMatrix
     * @param rowCount
     * @param falseCount
     * @param correctCount
     * @param labels
     * @param missingValueRowCount count of rows with missing values
     */
    public AccuracyScorerJobOutput(final int[][] confusionMatrix, final long rowCount, final int falseCount,
        final int correctCount, final List<Object> labels, final long missingValueRowCount) {
        super(missingValueRowCount);
        set(CONFUSION_MATRIX, confusionMatrix);
        set(ROW_COUNT, rowCount);
        set(FALSE_COUNT, falseCount);
        set(CORRECT_COUNT, correctCount);
        set(LABELS, labels);
    }

    /**
     * @return the confusion matrix
     */
    public int[][] getConfusionMatrix() {
        return get(CONFUSION_MATRIX);
    }

    /**
     * @return the row count
     */
    public long getRowCount() {
        return getLong(ROW_COUNT);
    }

    /**
     * @return the count of false predictions
     */
    public int getFalseCount() {
        return getInteger(FALSE_COUNT);
    }

    /**
     * @return the count of correct predictions
     */
    public int getCorrectCount() {
        return getInteger(CORRECT_COUNT);
    }

    /**
     * @return the labels
     */
    public List<Object> getLabels() {
        return get(LABELS);
    }
}
