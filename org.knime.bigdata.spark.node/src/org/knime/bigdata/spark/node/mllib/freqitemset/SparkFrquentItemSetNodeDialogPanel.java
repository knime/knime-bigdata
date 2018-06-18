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
 *   Created on Jun 4, 2018 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.spark.node.mllib.freqitemset;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.BoxLayout;
import javax.swing.JCheckBox;
import javax.swing.JOptionPane;
import javax.swing.JPanel;

import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.collection.CollectionDataValue;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnNameSelection;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.port.PortObjectSpec;

/**
 * Panel with frequent item sets settings.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class SparkFrquentItemSetNodeDialogPanel implements ActionListener {
    private final JPanel m_panel;
    private final SparkFrequentItemSetSettings m_settings;
    private final DialogComponentColumnNameSelection m_itemColumn;
    private final DialogComponentNumber m_minSupport;
    private final JCheckBox m_overwriteNumPartitions;
    private final DialogComponentNumber m_numPartitions;

    /**
     * Default constructor.
     * @param settings frequent item set settings model
     */
    @SuppressWarnings("unchecked")
    public SparkFrquentItemSetNodeDialogPanel(final SparkFrequentItemSetSettings settings) {
        m_settings = settings;
        m_itemColumn =
            new DialogComponentColumnNameSelection(m_settings.getItemsColumnModel(), "Item column:", 0, CollectionDataValue.class);
        m_minSupport =
            new DialogComponentNumber(m_settings.getMinSupportModel(), "Minimum support:", 0.1, 8);
        m_overwriteNumPartitions =
            new JCheckBox("Overwrite number of partitions");
        m_numPartitions =
            new DialogComponentNumber(m_settings.getNumPartitionsModel(), "Number of partitions:", 10);

        m_panel = new JPanel();
        m_panel.setLayout(new BoxLayout(m_panel, BoxLayout.PAGE_AXIS));
        m_panel.add(m_itemColumn.getComponentPanel());
        m_panel.add(m_minSupport.getComponentPanel());
        JPanel overwritePanel = new JPanel(); // required to center checkbox + label
        overwritePanel.add(m_overwriteNumPartitions);
        m_panel.add(overwritePanel);
        m_panel.add(m_numPartitions.getComponentPanel());
        m_overwriteNumPartitions.addActionListener(this);
    }

    /** @return panel containing all components */
    public JPanel getComponentPanel() {
        return m_panel;
    }

    /**
     * @param settings settings to write through
     * @throws InvalidSettingsException on invalid values
     */
    public void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_itemColumn.saveSettingsTo(settings);
        m_minSupport.saveSettingsTo(settings);
        m_numPartitions.saveSettingsTo(settings);
    }

    /**
     * @param settings settings to load
     * @param specs input data port specs
     * @throws NotConfigurableException on missing input ports
     */
    public void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs) throws NotConfigurableException {
        if (specs == null || specs.length < 1 || specs[0] == null) {
            throw new NotConfigurableException("Spark input data required.");
        }

        final DataTableSpec tableSpecs[] = new DataTableSpec[] {
            ((SparkDataPortObjectSpec) specs[0]).getTableSpec()
        };

        m_itemColumn.loadSettingsFrom(settings, tableSpecs);
        m_minSupport.loadSettingsFrom(settings, specs);
        m_numPartitions.loadSettingsFrom(settings, specs);

        m_overwriteNumPartitions.setSelected(m_settings.getNumPartitionsModel().isEnabled());

        m_settings.loadDefaults(tableSpecs[0]);
    }

    @Override
    public void actionPerformed(final ActionEvent e) {
        if (e.getSource().equals(m_overwriteNumPartitions)) {
            final boolean overwrite = m_overwriteNumPartitions.isSelected();
            m_settings.getNumPartitionsModel().setEnabled(overwrite);
            if (overwrite) {
                JOptionPane.showMessageDialog(m_panel,
                    "Wrong partition count might result in serious performance issues on huge data sets. Use with caution!",
                    "Warning", JOptionPane.WARNING_MESSAGE);
            }
        }
    }
}
