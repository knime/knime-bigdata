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

import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.JPanel;

import org.knime.bigdata.spark.node.mllib.freqitemset.SparkFrquentItemSetNodeDialogPanel;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.port.PortObjectSpec;

/**
 * Association rules learner dialog.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class SparkAssociationRuleLearnerNodeDialog extends NodeDialogPane {
    private final SparkAssociationRuleLearnerSettings m_settings;
    private final SparkFrquentItemSetNodeDialogPanel m_freqItems;
    private final DialogComponentNumber m_minConfidence;

    /** Default constructor. */
    public SparkAssociationRuleLearnerNodeDialog() {
        m_settings = new SparkAssociationRuleLearnerSettings();
        m_freqItems = new SparkFrquentItemSetNodeDialogPanel(m_settings);
        m_minConfidence =
            new DialogComponentNumber(m_settings.getMinConfidenceModel(), "Minimum confidence:", 0.1, 8);

        final JPanel panel = new JPanel();
        panel.setLayout(new BoxLayout(panel, BoxLayout.PAGE_AXIS));

        JPanel itemsPanel = m_freqItems.getComponentPanel();
        itemsPanel.setBorder(BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), "Frequent Item Sets"));
        panel.add(itemsPanel);

        JPanel rulesPanel = new JPanel();
        rulesPanel.setLayout(new BoxLayout(rulesPanel, BoxLayout.Y_AXIS));
        rulesPanel.setBorder(BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), "Association Rules"));
        rulesPanel.add(m_minConfidence.getComponentPanel());
        panel.add(rulesPanel);

        addTab("Settings", panel);
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_freqItems.saveSettingsTo(settings);
        m_minConfidence.saveSettingsTo(settings);
    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs) throws NotConfigurableException {
        m_freqItems.loadSettingsFrom(settings, specs);
        m_minConfidence.loadSettingsFrom(settings, specs);
    }
}
