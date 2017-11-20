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
package org.knime.bigdata.spark.node.preproc.missingval;

import org.knime.base.node.preproc.pmml.missingval.MissingCellHandlerFactory;
import org.knime.base.node.preproc.pmml.missingval.MissingValueHandlerPanel;
import org.knime.core.data.DataColumnSpec;

/**
 * Factory class for a missing spark value handlers.
 *
 * @author Sascha Wolke, KNIME GmbH
 * @since 3.5
 */
public abstract class SparkMissingValueHandlerFactory extends MissingCellHandlerFactory {
    @Override
    public MissingValueHandlerPanel getSettingsPanel() {
        return null;
    }

    @Override
    public boolean hasSettingsPanel() {
        return false;
    }

    @Override
    public abstract SparkMissingValueHandler createHandler(final DataColumnSpec column);
}
