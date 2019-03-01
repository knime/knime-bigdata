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

import org.knime.base.data.filter.row.dialog.OperatorParameters;
import org.knime.core.data.DataType;

/**
 * Spark SQL operator function with one parameter.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
class SparkOneParameterOperatorFunction extends AbstractSparkParameterOperatorFunction {
    private static final String FORMAT = "`%s` %s %s";
    private final String m_operator;

    public SparkOneParameterOperatorFunction(final String operator) {
        m_operator = operator;
    }

    @Override
    public String apply(final OperatorParameters parameters) {
        Objects.requireNonNull(parameters, "parameters");
        final String column = parameters.getColumnSpec().getName();
        final DataType dataType = parameters.getColumnSpec().getType();
        final String value = convertInputValue(parameters.getValues()[0], dataType);
        return String.format(FORMAT, column, m_operator, value);
    }
}