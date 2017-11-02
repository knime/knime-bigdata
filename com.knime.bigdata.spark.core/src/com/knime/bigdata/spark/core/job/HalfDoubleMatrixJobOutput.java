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
 *   Created on 16.05.2016 by koetter
 */
package com.knime.bigdata.spark.core.job;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class HalfDoubleMatrixJobOutput extends JobOutput {

    private static final String ROW_COUNT = "rowCount";
    private static final String STORES_DIAGONAL = "storesDiagonal";
    private static final String MATRIX = "matrix";

    /**
     *
     */
    public HalfDoubleMatrixJobOutput() {}

    /**
     * @param rowCount
     * @param storesDiagonal
     * @param matrix
     */
    public HalfDoubleMatrixJobOutput(final long rowCount, final boolean storesDiagonal, final double[] matrix) {
        set(ROW_COUNT, rowCount);
        set(STORES_DIAGONAL, storesDiagonal);
        set(MATRIX, matrix);
    }

    /**
     * @return the row count of the matrix
     */
    public long getRowCount() {
        return getInteger(ROW_COUNT);
    }

    /**
     * @return if the diagonal values are stored
     */
    public boolean storesDiagonal() {
        return get(STORES_DIAGONAL);
    }

    /**
     * @return the value half matrix as one dimensional array
     */
    public double[] getMatrix() {
        return get(MATRIX);
    }
}
