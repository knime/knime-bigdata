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
 *   Created on Nov 14, 2018 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.spark.node.preproc.filter.row.operator;

import org.knime.core.data.DataType;
import org.knime.core.data.def.BooleanCell.BooleanCellFactory;
import org.knime.core.data.def.DoubleCell.DoubleCellFactory;
import org.knime.core.data.def.IntCell.IntCellFactory;
import org.knime.core.data.def.LongCell.LongCellFactory;

/**
 * Abstract spark operator function with parameters.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
abstract class AbstractSparkParameterOperatorFunction implements SparkOperatorFunction {

    /**
     * Converts input value according to the provided data type.
     *
     * @param inputValue the input value
     * @param type the {@link DataType} of value
     * @return the converted value
     */
    String convertInputValue(final String inputValue, final DataType type) {
        if (BooleanCellFactory.TYPE.equals(type)) {
            return inputValue.toLowerCase();
        } else if (IntCellFactory.TYPE.equals(type)
                || LongCellFactory.TYPE.equals(type)
                || DoubleCellFactory.TYPE.equals(type)) {
            return inputValue;
        } else {
            final String escaped = inputValue.replaceAll("'", "\\\\'");
            return "'" + escaped + "'";
        }
    }
}
