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
package com.knime.bigdata.spark.node.preproc.missingval.compute;

import org.knime.base.node.preproc.pmml.missingval.MVSettings;
import org.knime.base.node.preproc.pmml.missingval.MissingCellHandlerFactoryManager;
import org.knime.base.node.preproc.pmml.missingval.compute.MissingValueHandlerNodeDialog;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;

import com.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import com.knime.bigdata.spark.node.preproc.missingval.SparkMissingValueHandlerFactoryManager;
import com.knime.bigdata.spark.node.preproc.missingval.SparkMissingValueSettings;

/**
 * Spark specific missing value handler dialog.
 * 
 * @author Sascha Wolke, KNIME GmbH
 */
public class SparkMissingValueNodeDialog extends MissingValueHandlerNodeDialog {

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {

        if (specs.length == 1 && specs[0] != null && specs[0] instanceof SparkDataPortObjectSpec) {
            SparkDataPortObjectSpec spec = (SparkDataPortObjectSpec)specs[0];
            super.loadSettingsFrom(settings, new PortObjectSpec[]{spec.getTableSpec()});
        } else {
            throw new NotConfigurableException("Input port not connected.");
        }
    }

    @Override
    protected MVSettings createEmptySettings() {
        return new SparkMissingValueSettings();
    }

    @Override
    protected MissingCellHandlerFactoryManager getHandlerFactoryManager() {
        return SparkMissingValueHandlerFactoryManager.getInstance();
    }
}
