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
 *   Created on Nov 16, 2017 by Sascha Wolke, KNIME GmbH
 */
package com.knime.bigdata.spark.node.sql_function;

import java.util.Comparator;

/**
 * Compares two {@link SparkSQLFunction}s based on their identifier.
 * @author Sascha Wolke, KNIME GmbH
 */
public class SparkSQLFunctionIdComperator implements Comparator<SparkSQLFunction> {

    /** Descending order comparator. */
    public static final SparkSQLFunctionIdComperator DESC = new SparkSQLFunctionIdComperator(-1);

    /** Ascending order comparator. */
    public static final SparkSQLFunctionIdComperator ASC = new SparkSQLFunctionIdComperator(1);

    private final int m_ascending;

    private SparkSQLFunctionIdComperator(final int manipulator) {
        m_ascending = manipulator;
    }

    @Override
    public int compare(final SparkSQLFunction o1, final SparkSQLFunction o2) {
        if (o1 == o2) {
            return 0;
        }
        if (o1 == null) {
            return 1 * m_ascending;
        }
        if (o2 == null) {
            return -1 * m_ascending;
        }
        return o1.getId().compareTo(o2.getId()) * m_ascending;
    }
}
