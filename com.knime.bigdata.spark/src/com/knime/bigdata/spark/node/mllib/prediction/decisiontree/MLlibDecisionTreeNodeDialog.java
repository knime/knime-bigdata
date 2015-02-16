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
 *
 * History
 *   Created on 12.02.2015 by koetter
 */
package com.knime.bigdata.spark.node.mllib.prediction.decisiontree;

import org.knime.core.data.DataTableSpec;
import org.knime.core.data.NominalValue;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnNameSelection;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.database.DatabasePortObjectSpec;

/**
 *
 * @author koetter
 */
public class MLlibDecisionTreeNodeDialog extends NodeDialogPane {
    private final DialogComponentColumnNameSelection m_column =
            new DialogComponentColumnNameSelection(MLlibDecisionTreeNodeModel.createClassColModel(),
            "Class column ", 0, NominalValue.class);

    private final DialogComponent[] m_components = new DialogComponent[] {m_column};

    /**
     *
     */
    public MLlibDecisionTreeNodeDialog() {

        super.addTab("Settings", m_column.getComponentPanel());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings,
            final PortObjectSpec[] ports) throws NotConfigurableException {
        DatabasePortObjectSpec dbSpec = (DatabasePortObjectSpec) ports[0];
        final DataTableSpec[] specs;
        if (dbSpec == null) {
            specs = new DataTableSpec[]{null};
        } else {
            specs = new DataTableSpec[]{dbSpec.getDataTableSpec()};
        }
        for (DialogComponent c : m_components) {
            c.loadSettingsFrom(settings, specs);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings)
            throws InvalidSettingsException {
        for (DialogComponent c : m_components) {
            c.saveSettingsTo(settings);
        }
    }
}
