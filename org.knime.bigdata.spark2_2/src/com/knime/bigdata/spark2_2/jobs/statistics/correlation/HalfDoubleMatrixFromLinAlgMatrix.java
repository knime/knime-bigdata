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
 *   Created on 26.09.2015 by dwk
 */
package com.knime.bigdata.spark2_2.jobs.statistics.correlation;

import java.io.Serializable;

import org.apache.spark.mllib.linalg.Matrix;

import com.knime.bigdata.spark.core.job.HalfDoubleMatrixJobOutput;
import com.knime.bigdata.spark.core.job.SparkClass;

/**
 *
 * @author Thorsten Meinl, University of Konstanz (original version in org.knime.base.util),
 * modified by dwk
 */

/**
 * This stores half a matrix of doubles efficiently in just one array. The access function {@link #get(int, int)} works
 * symmetrically. Upon creating the matrix you can choose if place for the diagonal should be reserved or not.
 *
 * It is also possible to save the contents of the matrix into a node settings object and load it again from there
 * afterwards.
 *
 * The maximum number of rows/column that the matrix may contain is 65,500.
 *
 */
@SparkClass
public final class HalfDoubleMatrixFromLinAlgMatrix implements Serializable {
    private static final long serialVersionUID = 1L;

    private final boolean m_withDiagonal;

    private final double[] m_matrix;

    /**
     * @return {@link HalfDoubleMatrixJobOutput} with the HalfDoubleMatrix representation of the given input {@link Matrix}
     */
    public HalfDoubleMatrixJobOutput getJobOutput() {
        return new HalfDoubleMatrixJobOutput(getRowCount(), storesDiagonal(), getMatrixArray());
    }

    /**
     * Creates a new half-matrix of doubles.
     *
     * @param aDiagonalMatrix the Matrix to be converted to a HalfDoubleMatrix
     * @param withDiagonal <code>true</code> if the diagonal should be stored too, <code>false</code> otherwise
     */
    public HalfDoubleMatrixFromLinAlgMatrix(final Matrix aDiagonalMatrix, final boolean withDiagonal) {
        m_withDiagonal = withDiagonal;
        final long size;
        final long rows = aDiagonalMatrix.numRows();
        if (withDiagonal) {
            size = (rows * rows + rows) / 2;
        } else {
            size = (rows * rows - rows) / 2;
        }
        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Too many rows, only " + Integer.MAX_VALUE + " rows are possible");
        }
        m_matrix = new double[(int)size];

        for (int r=0; r < rows; r++) {
            for (int c = r; c < rows; c++) {
                if (c != r || withDiagonal) {
                    set(r, c, aDiagonalMatrix.apply(r, c));
                }
            }
        }
    }

    /**
     * Sets a value in the matrix. This function works symmetrically, i.e. <code>set(i, j, 1)</code> is the same as
     * <code>set(j, i, 1)</code>.
     *
     * @param row the value's row
     * @param col the value's column
     * @param value the value
     */
    public void set(final int row, final int col, final double value) {
        if (!m_withDiagonal && row == col) {
            throw new IllegalArgumentException("Can't set value in diagonal " + "of the matrix (no space reserved)");
        }
        if (row > col) {
            if (m_withDiagonal) {
                m_matrix[row * (row + 1) / 2 + col] = value;
            } else {
                m_matrix[row * (row - 1) / 2 + col] = value;
            }
        } else {
            if (m_withDiagonal) {
                m_matrix[col * (col + 1) / 2 + row] = value;
            } else {
                m_matrix[col * (col - 1) / 2 + row] = value;
            }
        }
    }

    /**
     * Returns a value in the matrix. This function works symmetrically, i.e. <code>get(i, j)</code> is the same as
     * <code>get(j, i)</code>.
     *
     * @param row the value's row
     * @param col the value's column
     * @return the value
     */
    public double get(final int row, final int col) {
        if (!m_withDiagonal && row == col) {
            throw new IllegalArgumentException("Can't read value in diagonal " + "of the matrix (not saved)");
        }
        if (row > col) {
            if (m_withDiagonal) {
                return m_matrix[row * (row + 1) / 2 + col];
            } else {
                return m_matrix[row * (row - 1) / 2 + col];
            }
        } else {
            if (m_withDiagonal) {
                return m_matrix[col * (col + 1) / 2 + row];
            } else {
                return m_matrix[col * (col - 1) / 2 + row];
            }
        }
    }

    /**
     * @return matrix as double array
     */
    public double[] getMatrixArray() {
        return m_matrix;
    }

    /**
     * Returns if the half matrix also stores the diagonal or not.
     *
     * @return <code>true</code> if the diagonal is stored, <code>false</code> otherwise
     */
    public boolean storesDiagonal() {
        return m_withDiagonal;
    }

    /**
     * Returns the number of rows the half matrix has.
     *
     * @return the number of rows
     */
    public int getRowCount() {
        if (m_withDiagonal) {
            return (-1 + (int)Math.sqrt(1 + 8 * m_matrix.length)) / 2;
        } else {
            return (1 + (int)Math.sqrt(1 + 8 * m_matrix.length)) / 2;
        }
    }

}
