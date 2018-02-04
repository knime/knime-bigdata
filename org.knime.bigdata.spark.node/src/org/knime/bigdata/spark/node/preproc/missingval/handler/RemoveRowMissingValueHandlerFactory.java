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
 */
package org.knime.bigdata.spark.node.preproc.missingval.handler;

import org.knime.base.node.preproc.pmml.missingval.handlers.RemoveRowMissingCellHandlerFactory;
import org.knime.bigdata.spark.node.preproc.missingval.SparkMissingValueHandler;
import org.knime.bigdata.spark.node.preproc.missingval.SparkMissingValueHandlerFactory;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataType;

/**
 * Creates a handler that removes rows with a missing values.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class RemoveRowMissingValueHandlerFactory extends SparkMissingValueHandlerFactory {

    /** We have to use the KNIME cell handler ID here to produce compatible PMML. */
    public final static String ID = RemoveRowMissingCellHandlerFactory.ID;

    @Override
    public String getID() {
        return ID;
    }

    @Override
    public String getDisplayName() {
        return "Remove Row";
    }

    @Override
    public SparkMissingValueHandler createHandler(final DataColumnSpec column) {
        return new RemoveRowMissingValueHandler(column);
    }

    @Override
    public boolean producesPMML4_2() {
        return false;
    }

    @Override
    public boolean isApplicable(final DataType type) {
        return true;
    }
}
