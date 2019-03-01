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
 * Spark SQL between operator function.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
class SparkBetweenOperatorFunction extends AbstractSparkParameterOperatorFunction {
    private static final String FORMAT = "`%s` BETWEEN %s AND %s";

    @Override
    public String apply(final OperatorParameters parameters) {
        Objects.requireNonNull(parameters, "parameters");
        final String column = parameters.getColumnSpec().getName();
        final DataType dataType = parameters.getColumnSpec().getType();
        final String[] values = parameters.getValues();
        final String firstValue = convertInputValue(values[0], dataType);
        final String secondValue = convertInputValue(values[1], dataType);
        return String.format(FORMAT, column, firstValue, secondValue);
    }
}