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
 *   Created on Nov 5, 2018 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.spark.node.preproc.filter.row;

import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;

import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.node.preproc.filter.row.operator.SparkOperatorRegistry;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.rowfilter.OperatorPanelParameters;
import org.knime.core.node.rowfilter.component.DefaultRowFilterElementFactory;
import org.knime.core.node.rowfilter.component.RowFilterComponent;
import org.knime.core.node.rowfilter.component.RowFilterElementFactory;

/**
 * Spark row filter node dialog.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class SparkRowFilterNodeDialog extends NodeDialogPane {

    private final SparkRowFilterSettings m_settings = new SparkRowFilterSettings();

    private final RowFilterElementFactory m_elementFactory = new DefaultRowFilterElementFactory();

    private final RowFilterComponent m_rowFilter = new RowFilterComponent(m_settings.getRowFilterConfig(), m_elementFactory);

    /** Default constructor. */
    public SparkRowFilterNodeDialog() {
        final JPanel panel = m_rowFilter.getPanel();
        panel.setBorder(new EmptyBorder(2, 2, 2, 2));

        super.addTab("Conditions", panel);
    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings,
            final PortObjectSpec[] specs) throws NotConfigurableException {

        if (specs == null || specs.length <= 0 || specs[0] == null) {
            throw new NotConfigurableException("No input Spark data available");
        }

        final DataTableSpec spec = ((SparkDataPortObjectSpec)specs[0]).getTableSpec();
        final OperatorPanelParameters parameters = new OperatorPanelParameters();
        final SparkOperatorRegistry operatorRegistry = SparkOperatorRegistry.getInstance();

        try {
            m_rowFilter.loadSettingsFrom(settings, spec, parameters, operatorRegistry);
        } catch (InvalidSettingsException e) {
            throw new NotConfigurableException(e.getMessage());
        }
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_rowFilter.saveSettingsTo(settings);
    }
}
