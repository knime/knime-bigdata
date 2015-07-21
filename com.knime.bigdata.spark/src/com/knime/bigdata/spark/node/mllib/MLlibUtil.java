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
 *   Created on 21.07.2015 by koetter
 */
package com.knime.bigdata.spark.node.mllib;

import java.util.LinkedList;
import java.util.List;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;

/**
 *
 * @author koetter
 */
public final class MLlibUtil {

    /**
     * Prevent object creation.
     */
    private MLlibUtil() {}

    /**
     * @param tableSpec the {@link DataTableSpec}
     * @param colNames the column names to get the indices for
     * @return the indices of the columns in the same order as in the input list
     * @throws InvalidSettingsException if the input list is empty or a column name could not be found in the input spec
     */
    public static List<Integer> getColumnIndices(final DataTableSpec tableSpec, final List<String> colNames)
        throws InvalidSettingsException {
        if (colNames == null || colNames.isEmpty()) {
            throw new InvalidSettingsException("No columns selected");
        }
        final List<Integer> numericColIdx = new LinkedList<>();
        for (String numericColName : colNames) {
            final int colIdx = tableSpec.findColumnIndex(numericColName);
            if (colIdx < 0) {
                throw new InvalidSettingsException("Column: " + numericColName + " not found in input data");
            }
            numericColIdx.add(Integer.valueOf(colIdx));
        }
        return numericColIdx;
    }

    /**
     * @param tableSpec the {@link DataTableSpec}
     * @param featureColNames the column names to get the indices for
     * @return the indices of the columns in the same order as in the input list
     * @throws InvalidSettingsException if the input list is empty or a column name could not be found in the input spec
     */
    public static int[] getColumnIndices(final DataTableSpec tableSpec, final String[] featureColNames)
        throws InvalidSettingsException {
        if (featureColNames == null || featureColNames.length < 1) {
            throw new InvalidSettingsException("No columns selected");
        }
        int[] colIdxs = new int[featureColNames.length];
        for (int i = 0, length = featureColNames.length; i < length; i++) {
            final String colName = featureColNames[i];
            final int colIdx = tableSpec.findColumnIndex(colName);
            if (colIdx < 0) {
                throw new InvalidSettingsException("Column: " + colName + " not found in input data");
            }
            colIdxs[i] = colIdx;
        }
        return colIdxs;
    }
}
