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
package com.knime.bigdata.spark.node.scorer.accuracy;

import java.util.List;

import com.knime.bigdata.spark.core.job.JobOutput;
import com.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Ole Ostergaard
 */
@SparkClass
public class ScorerJobOutput extends JobOutput {

    private static final String CONFUSION_MATRIX = "confusionMatrix";
    private static final String ROW_COUNT = "rowCount";
    private static final String FALSE_COUNT = "falseCount";
    private static final String CORRECT_COUNT = "correctCount";
    private static final String CLASS_COL = "classCol";
    private static final String PREDICTION_COL = "predictionCol";
    private static final String LABELS = "labels";


    /**
     * Paramless constructor for automatic deserialization.
     */
    public ScorerJobOutput() {}

    /**
     * @param confusionMatrix
     * @param rowCount
     * @param falseCount
     * @param correctCount
     * @param classCol
     * @param predictionCol
     * @param labels
     */
    public ScorerJobOutput(final int[][] confusionMatrix, final long rowCount, final int falseCount, final int correctCount, final Integer classCol,
        final Integer predictionCol, final List<Object> labels) {
        set(CONFUSION_MATRIX, confusionMatrix);
        set(ROW_COUNT, rowCount);
        set(FALSE_COUNT, falseCount);
        set(CORRECT_COUNT, correctCount);
        set(CLASS_COL, classCol);
        set(PREDICTION_COL, predictionCol);
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

    /**
     * @return the labels
     */
    public List<Object> getLabels() {
        return get(LABELS);
    }
}
