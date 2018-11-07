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
 *   Created on Nov 6, 2018 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.spark.node.preproc.filter.row.operator;

import java.util.Objects;

import org.knime.core.node.rowfilter.OperatorParameters;

/**
 * Spark SQL operator function without parameters.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
class SparkNoParameterOperatorFunction implements SparkOperatorFunction {
    private static final String FORMAT = "`%s` %s";
    private final String m_operator;

    public SparkNoParameterOperatorFunction(final String operator) {
        m_operator = operator;
    }

    @Override
    public String apply(final OperatorParameters parameters) {
        Objects.requireNonNull(parameters, "parameters");
        final String column = parameters.getColumnSpec().getName();
        return String.format(FORMAT, column, m_operator);
    }
}