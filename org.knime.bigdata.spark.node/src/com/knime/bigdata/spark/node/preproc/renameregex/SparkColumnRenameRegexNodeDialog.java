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
 *   Created on 28.06.2015 by koetter
 */
package com.knime.bigdata.spark.node.preproc.renameregex;

import org.knime.base.node.preproc.columnrenameregex.ColumnRenameRegexNodeDialogPane;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;

import com.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkColumnRenameRegexNodeDialog extends ColumnRenameRegexNodeDialogPane {

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
            throws NotConfigurableException {
        final SparkDataPortObjectSpec sparkSpec = (SparkDataPortObjectSpec) specs[0];
        if (sparkSpec != null) {
            super.loadSettingsFrom(settings, new DataTableSpec[] {sparkSpec.getTableSpec()});
            return;
        }
        throw new NotConfigurableException("No input connection available");
    }
}
