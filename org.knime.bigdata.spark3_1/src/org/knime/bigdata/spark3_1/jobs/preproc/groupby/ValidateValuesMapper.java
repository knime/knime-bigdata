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
 *   Created on Apr 5, 2018 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.spark3_1.jobs.preproc.groupby;

import java.util.HashSet;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Validates that all values of a given column are included in a given values list or fails otherwise.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
@SparkClass
public class ValidateValuesMapper implements MapFunction<Row, Row> {
    private static final long serialVersionUID = 1L;

    private final int m_columnIndex;
    private final HashSet<Object> m_values;

    /**
     * Validates that all values of a given column are included in a given values list or fails otherwise.
     *
     * @param columnIndex column to validate
     * @param valuesList list of valid values
     */
    public ValidateValuesMapper(final int columnIndex, final List<Object> valuesList) {
        m_columnIndex = columnIndex;
        m_values = new HashSet<>(valuesList);
    }

    @Override
    public Row call(final Row row) throws Exception {
        if (!m_values.contains(row.get(m_columnIndex))) {
            throw new KNIMESparkException(
                String.format("Provided pivot values do not contain value '%s'.", row.get(m_columnIndex)));
        }

        return row;
    }
}
