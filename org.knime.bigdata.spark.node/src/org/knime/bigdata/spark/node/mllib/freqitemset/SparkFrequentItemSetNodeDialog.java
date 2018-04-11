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
 *   Created on Jan 30, 2018 by Sascha Wolke, KNIME GmbH
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
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnNameSelection;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.port.PortObjectSpec;

/**
 * Frequent item sets settings dialog.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class SparkFrequentItemSetNodeDialog extends NodeDialogPane implements ActionListener {
    private final SparkFrequentItemSetSettings m_settings = new SparkFrequentItemSetSettings();

    @SuppressWarnings("unchecked")
    private final DialogComponentColumnNameSelection m_itemColumn =
            new DialogComponentColumnNameSelection(m_settings.getItemsColumnModel(), "Item column:", 0, CollectionDataValue.class);

    private final DialogComponentNumber m_minSupport =
            new DialogComponentNumber(m_settings.getMinSupportModel(), "Minimum support:", 0.1);

    private final JCheckBox m_overwriteNumPartitions =
            new JCheckBox("Overwrite number of partitions");

    private final DialogComponentNumber m_numPartitions =
            new DialogComponentNumber(m_settings.getNumPartitionsModel(), "Number of partitions:", 10);

    /** Default constructor. */
    public SparkFrequentItemSetNodeDialog() {
        final JPanel panel = new JPanel();
        panel.setLayout(new BoxLayout(panel, BoxLayout.PAGE_AXIS));

        panel.add(m_itemColumn.getComponentPanel());
        panel.add(m_minSupport.getComponentPanel());
        JPanel overwritePanel = new JPanel(); // required to center checkbox + label
        overwritePanel.add(m_overwriteNumPartitions);
        panel.add(overwritePanel);
        panel.add(m_numPartitions.getComponentPanel());
        m_overwriteNumPartitions.addActionListener(this);

        addTab("Settings", panel);
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_itemColumn.saveSettingsTo(settings);
        m_minSupport.saveSettingsTo(settings);
        m_numPartitions.saveSettingsTo(settings);
    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs) throws NotConfigurableException {
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

        m_settings.loadDefaults(tableSpecs);
    }

    @Override
    public void actionPerformed(final ActionEvent e) {
        if (e.getSource().equals(m_overwriteNumPartitions)) {
            final boolean overwrite = m_overwriteNumPartitions.isSelected();
            m_settings.getNumPartitionsModel().setEnabled(overwrite);
            if (overwrite) {
                JOptionPane.showMessageDialog(getPanel(),
                    "Wrong partition count might result in serious performance issues on huge data sets. Use with caution!",
                    "Warning", JOptionPane.WARNING_MESSAGE);
            }
        }
    }
}
