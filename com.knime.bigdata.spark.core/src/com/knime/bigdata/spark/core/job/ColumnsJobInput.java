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
 *   Created on 07.05.2016 by koetter
 */
package com.knime.bigdata.spark.core.job;

import java.util.Arrays;
import java.util.List;

/**
 * {@link JobInput} implementation that stores a list of column indices that should be considered in
 * the Spark job.
 *
 * @author Tobias Koetter, KNIME.com
 */
@SparkClass
public class ColumnsJobInput extends JobInput {

    private static final String KEY_FEATURE_COLUMN_INDICES = "colIdxs";

    /**
     * Paramless constructor for automatic deserialization.
     */
    public ColumnsJobInput() {}


    /**
     * @param namedInputObject the input data to learn from
     * @param colIdxs the feature column indices starting with 0
     */
    public ColumnsJobInput(final String namedInputObject, final Integer... colIdxs) {
        this(namedInputObject, null, colIdxs);
    }

    /**
     * @param namedInputObject the input data to learn from
     * @param namedOuputObject optional output object name
     * @param colIdxs the feature column indices starting with 0
     */
    public ColumnsJobInput(final String namedInputObject, final String namedOuputObject, final Integer... colIdxs) {
        this(namedInputObject, namedOuputObject, Arrays.asList(colIdxs));
    }
    /**
     * @param namedInputObject the input data to learn from
     * @param colIdxs the feature column indices starting with 0
     */
    public ColumnsJobInput(final String namedInputObject, final List<Integer> colIdxs) {
        this(namedInputObject, null, colIdxs);
    }
    /**
     * @param namedInputObject the input data to learn from
     * @param namedOuputObject optional output object name
     * @param colIdxs the feature column indices starting with 0
     */
    public ColumnsJobInput(final String namedInputObject, final String namedOuputObject, final List<Integer> colIdxs) {
        addNamedInputObject(namedInputObject);
        if (namedOuputObject != null) {
            addNamedOutputObject(namedOuputObject);
        }
        set(KEY_FEATURE_COLUMN_INDICES, colIdxs);
    }

    /**
     * @return the column indices starting with 0
     */
    public List<Integer> getColumnIdxs() {
        return get(KEY_FEATURE_COLUMN_INDICES);
    }

    /**
     * @param columns - All column names in data frame
     * @return The column names that should be considered in the Spark job.
     */
    public String[] getColumnNames(final String columns[]) {
        final List<Integer> columnIndices = getColumnIdxs();
        final String columnNames[] = new String[columnIndices.size()];
        for (int i = 0; i < columnIndices.size(); i++) {
            columnNames[i] = columns[columnIndices.get(i)];
        }
        return columnNames;
    }
}