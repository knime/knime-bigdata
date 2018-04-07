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
package org.knime.bigdata.spark.node.mllib.associationrule;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.JCheckBox;
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
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.port.PortObjectSpec;

/**
 * Association rules apply dialog.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class AssociationRuleApplyNodeDialog extends NodeDialogPane implements ActionListener {
    private final AssociationRuleApplySettings m_settings = new AssociationRuleApplySettings();

    @SuppressWarnings("unchecked")
    private final DialogComponentColumnNameSelection m_itemColumn =
            new DialogComponentColumnNameSelection(m_settings.getItemColumnModel(), "Item column:", 1, CollectionDataValue.class);

    private final JCheckBox m_useRuleLimit =
            new JCheckBox("Limit number of used rules");
    private final DialogComponentNumber m_ruleLimit =
            new DialogComponentNumber(m_settings.getRuleLimitModel(), "Rule limit:", 1000);

    private final DialogComponentString m_outputColumn =
            new DialogComponentString(m_settings.getOutputColumnModel(), "Output column:");

    /** Default constructor. */
    public AssociationRuleApplyNodeDialog() {
        final JPanel tab = new JPanel();
        tab.setLayout(new BoxLayout(tab, BoxLayout.PAGE_AXIS));

        JPanel rulesPanel = new JPanel();
        rulesPanel.setLayout(new BoxLayout(rulesPanel, BoxLayout.Y_AXIS));
        rulesPanel.setBorder(BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), "Association Rules input"));
        JPanel useRulesPanel = new JPanel();
        useRulesPanel.add(m_useRuleLimit);
        m_useRuleLimit.addActionListener(this);
        rulesPanel.add(useRulesPanel);
        rulesPanel.add(m_ruleLimit.getComponentPanel());
        tab.add(rulesPanel);

        JPanel itemsPanel = new JPanel();
        itemsPanel.setLayout(new BoxLayout(itemsPanel, BoxLayout.Y_AXIS));
        itemsPanel.setBorder(BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), "Items input"));
        itemsPanel.add(m_itemColumn.getComponentPanel());
        tab.add(itemsPanel);

        JPanel outputPanel = new JPanel();
        outputPanel.setLayout(new BoxLayout(outputPanel, BoxLayout.Y_AXIS));
        outputPanel.setBorder(BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), "Output"));
        outputPanel.add(m_outputColumn.getComponentPanel());
        tab.add(outputPanel);

        addTab("Settings", tab);
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_ruleLimit.saveSettingsTo(settings);
        m_itemColumn.saveSettingsTo(settings);
        m_outputColumn.saveSettingsTo(settings);
    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs) throws NotConfigurableException {
        if (specs == null || specs.length < 2 || specs[0] == null || specs[1] == null) {
            throw new NotConfigurableException("Spark input data required.");
        }

        final DataTableSpec itemsTableSpec = ((SparkDataPortObjectSpec) specs[1]).getTableSpec();
        final DataTableSpec tablesSpecs[] = new DataTableSpec[] { null, itemsTableSpec };

        m_ruleLimit.loadSettingsFrom(settings, tablesSpecs);
        m_itemColumn.loadSettingsFrom(settings, tablesSpecs);
        m_outputColumn.loadSettingsFrom(settings, tablesSpecs);

        m_useRuleLimit.setSelected(m_settings.getRuleLimitModel().isEnabled());

        m_settings.loadDefaults(itemsTableSpec);
    }

    @Override
    public void actionPerformed(final ActionEvent e) {
        if (e.getSource().equals(m_useRuleLimit)) {
            m_settings.getRuleLimitModel().setEnabled(m_useRuleLimit.isSelected());
        }
    }
}
