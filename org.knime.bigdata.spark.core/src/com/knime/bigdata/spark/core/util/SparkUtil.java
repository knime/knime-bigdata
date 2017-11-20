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
 *   Created on 21.07.2015 by koetter
 */
package com.knime.bigdata.spark.core.util;

import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;

/**
 *
 * @author koetter
 */
public final class SparkUtil {

    /**
     * Prevent object creation.
     */
    private SparkUtil() {}

    /**
     * @param tableSpec the {@link DataTableSpec}
     * @param colNames the column names to get the indices for
     * @return the indices of the columns in the same order as in the input list
     * @throws InvalidSettingsException if the input list is empty or a column name could not be found in the input spec
     */
    public static Integer[] getColumnIndices(final DataTableSpec tableSpec, final List<String> colNames)
        throws InvalidSettingsException {
        if (colNames == null || colNames.isEmpty()) {
            throw new InvalidSettingsException("No columns selected");
        }
        final Integer[] numericColIdx = new Integer[colNames.size()];
        int idx = 0;
        for (String numericColName : colNames) {
            final int colIdx = tableSpec.findColumnIndex(numericColName);
            if (colIdx < 0) {
                throw new InvalidSettingsException("Column: " + numericColName + " not found in input data");
            }
            numericColIdx[idx++] = Integer.valueOf(colIdx);
        }
        return numericColIdx;
    }

    /**
     * @param tableSpec the {@link DataTableSpec}
     * @param colNames the column names to get the indices for
     * @return the indices of the columns in the same order as in the input list
     * @throws InvalidSettingsException if the input list is empty or a column name could not be found in the input spec
     */
    public static Integer[] getColumnIndices(final DataTableSpec tableSpec, final String... colNames)
        throws InvalidSettingsException {
        if (colNames == null || colNames.length < 1) {
            throw new InvalidSettingsException("No columns selected");
        }
        final Integer[] colIdxs = new Integer[colNames.length];
        for (int i = 0, length = colNames.length; i < length; i++) {
            final String colName = colNames[i];
            final int colIdx = tableSpec.findColumnIndex(colName);
            if (colIdx < 0) {
                throw new InvalidSettingsException("Column: " + colName + " not found in input data");
            }
            colIdxs[i] = colIdx;
        }
        return colIdxs;
    }


    /**
     * @param vals the primitive booleans to convert to Object array
     * @return the {@link Boolean} array representation
     */
    public static Boolean[] convert(final boolean[] vals) {
        return ArrayUtils.toObject(vals);
    }

    /**
     * @param vals the Integer vals to convert to int
     * @return the int array
     */
    public static int[] convert(final Integer[] vals) {
        return ArrayUtils.toPrimitive(vals);
    }
}
