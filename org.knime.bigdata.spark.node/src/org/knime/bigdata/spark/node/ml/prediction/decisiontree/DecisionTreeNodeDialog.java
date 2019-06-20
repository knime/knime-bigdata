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
 *   Created on 12.02.2015 by koetter
 */
package org.knime.bigdata.spark.node.ml.prediction.decisiontree;

import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.LayoutManager;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.Box;
import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JSeparator;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.bigdata.spark.core.node.MLlibNodeSettings;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;

/**
 * Dialog for decision tree-based model learners. Without any modifications, it displays the dialog for the (simple)
 * decision tree learner nodes, however it can be subclassed for other tree-based models.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 * @param <SETTINGS> The settings class to use.
 * @param <COMPONENTS> The components class to use.
 */
public class DecisionTreeNodeDialog<SETTINGS extends DecisionTreeSettings, COMPONENTS extends DecisionTreeComponents<SETTINGS>>
    extends NodeDialogPane {

    private final DecisionTreeLearnerMode m_mode;

    private final SETTINGS m_settings;

    private final COMPONENTS m_components;

    /**
     * @param components The components to use.
     */
    public DecisionTreeNodeDialog(final COMPONENTS components) {
        m_mode = components.getMode();
        m_settings = components.getSettings();
        m_components = components;

        addTab("Settings", getSettingsTab());

        final JPanel advancedTab = getAdvancedTab();
        if (advancedTab != null) {
            addTab("Advanced", advancedTab);
        }
    }

    /**
     *
     * @return a panel that will be displayed as the advanced tab. May be null, in which case no advanced tab will be
     *         displayed.
     */
    protected JPanel getAdvancedTab() {
        if (m_mode == DecisionTreeLearnerMode.DEPRECATED) {
            return null;
        }

        final JPanel tab = new JPanel(new GridBagLayout());

        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(5, 5, 5, 5);
        gbc.anchor = GridBagConstraints.NORTHWEST;
        gbc.fill = GridBagConstraints.NONE;
        gbc.gridx = 0;
        gbc.gridy = 0;

        addSeedingOption(tab, gbc);
        addVerticalGlue(tab, gbc);

        return tab;
    }

    /**
     *
     * @return a panel that will be displayed as the settings tab.
     */
    protected JPanel getSettingsTab() {
        final JPanel settingsTab = new JPanel(new GridBagLayout());

        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(5, 5, 5, 5);
        gbc.anchor = GridBagConstraints.NORTHWEST;
        gbc.fill = GridBagConstraints.NONE;
        gbc.gridx = 0;
        gbc.gridy = 0;

        addColumnSelections(settingsTab, gbc);

        addSeparatorAndLabel(settingsTab, "Tree Options", gbc);
        addTreeOptions(settingsTab, gbc);

        return settingsTab;
    }

    /**
     * Adds some vertically expanding glue. Meant to be use at the bottom of a tab.
     * @param settingsTab
     * @param gbc
     */
    protected void addVerticalGlue(final JPanel settingsTab, final GridBagConstraints gbc) {
        gbc.gridy++;
        gbc.anchor = GridBagConstraints.NORTHWEST;
        gbc.fill = GridBagConstraints.VERTICAL;
        gbc.gridx = 0;
        gbc.weighty = 10;
        settingsTab.add(Box.createVerticalGlue(), gbc);
        gbc.weighty = 0;
        gbc.fill = GridBagConstraints.NONE;
    }

    /**
     * @param panel
     * @param gbc
     */
    protected void addSeedingOption(final JPanel panel, final GridBagConstraints gbc) {
        gbc.gridy++;

        final JButton seedButton = new JButton("New");
        seedButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(final ActionEvent e) {
                m_settings.nextSeed();
            }
        });

        eliminateHGapIfPossible(m_components.getSeedComponent().getComponentPanel());
        final JPanel seedPanel = new JPanel(new FlowLayout(FlowLayout.LEFT, 0, 0));
        seedPanel.add(m_components.getSeedComponent().getComponentPanel());
        seedPanel.add(seedButton);
        m_settings.getUseStaticSeedModel().addChangeListener(new ChangeListener() {
            @Override
            public void stateChanged(final ChangeEvent e) {
                seedButton.setEnabled(m_settings.getUseStaticSeedModel().getBooleanValue());
            }
        });
        eliminateHGapIfPossible(m_components.getUseStaticSeedComponent().getComponentPanel());

        addLine(panel, m_components.getUseStaticSeedComponent().getComponentPanel(), seedPanel, gbc);
    }

    /**
     * Invoked to add "Tree options" to the settings tab.
     *
     * @param panel
     * @param gbc
     */
    protected void addTreeOptions(final JPanel panel, final GridBagConstraints gbc) {
        // is classification
        if (m_mode == DecisionTreeLearnerMode.DEPRECATED) {
            gbc.gridy++;
            addLine(panel, "Max number of bins", m_components.getMaxNoOfBinsComponent().getComponentPanel(), gbc);

            gbc.gridy++;
            addLine(panel, "Is classification", m_components.getIsClassificationComponent().getComponentPanel(), gbc);
        }

        // quality measure
        if (m_mode == DecisionTreeLearnerMode.DEPRECATED || m_mode == DecisionTreeLearnerMode.CLASSIFICATION) {
            gbc.gridy++;
            addLine(panel, "Quality measure", m_components.getQualityMeasureComponent().getComponentPanel(), gbc);
        }

        // max tree depth
        gbc.gridy++;
        addLine(panel, "Max tree depth", m_components.getMaxDepthComponent().getComponentPanel(), gbc);

        if (m_mode != DecisionTreeLearnerMode.DEPRECATED) {
            // rows per tree node
            gbc.gridy++;
            addLine(panel, "Min rows per tree node", m_components.getMinRowsPerChildComponent().getComponentPanel(), gbc);

            // min information gain
            gbc.gridy++;
            addLine(panel, "Min information gain per split",
                m_components.getMinInformationGainComponent().getComponentPanel(), gbc);

            gbc.gridy++;
            addLine(panel, "Max number of bins", m_components.getMaxNoOfBinsComponent().getComponentPanel(), gbc);
        }
    }

    /**
     * Adds a horizontal separator and a caption label.
     *
     * @param panel
     * @param label
     * @param gbc
     */
    protected void addSeparatorAndLabel(final JPanel panel, final String label, final GridBagConstraints gbc) {
        // separator
        gbc.gridx = 0;
        gbc.gridy++;
        gbc.weightx = 1.0;
        gbc.gridwidth = GridBagConstraints.REMAINDER;
        gbc.anchor = GridBagConstraints.CENTER;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        panel.add(new JSeparator(), gbc);
        gbc.weightx = 0;
        gbc.gridwidth = 1;
        gbc.anchor = GridBagConstraints.NORTHWEST;
        gbc.fill = GridBagConstraints.NONE;

        // tree options label
        gbc.gridy++;
        panel.add(new JLabel(label), gbc);
    }

    /**
     * Invoked to add target and feature colum selections to the settings tab.
     *
     * @param panel
     * @param gbc
     */
    protected void addColumnSelections(final JPanel panel, final GridBagConstraints gbc) {
        // class column
        gbc.gridx = 0;
        addLine(panel, "Target column", m_components.getClassColComponent().getComponentPanel(), gbc);

        // feature columns
        gbc.gridy++;
        gbc.fill = GridBagConstraints.BOTH;
        gbc.gridwidth = GridBagConstraints.REMAINDER;
        gbc.weightx = 1;
        gbc.weighty = 1;
        panel.add(m_components.getFeatureColsComponent().getComponentPanel(), gbc);
        gbc.fill = GridBagConstraints.NONE;
        gbc.gridwidth = 1;
        gbc.weightx = 0.0;
        gbc.weighty = 0.0;
    }

    /**
     * Adds a "settings line" to the given panel, which consists of the given label (left side), horizontally expanding
     * glue (middle), and the given component (right side).
     *
     * @param panel The panel to add to. Its layout is assumed to be a GridBayLayout.
     * @param label The label to display on the left side.
     * @param rightComponent The component to display on the right side.
     * @param gbc GridBagConstraints to use.
     */
    protected static void addLine(final JPanel panel, final String label, final JPanel rightComponent,
        final GridBagConstraints gbc) {

        addLine(panel, new JLabel(label), rightComponent, gbc);
    }

    /**
     * Adds a "settings line" to the given panel, which consists of the given component (left side), horizontally
     * expanding glue (middle), and another component (right side).
     *
     * @param panel The panel to add to. Its layout is assumed to be a GridBayLayout.
     * @param leftComponent The component to display on the left side.
     * @param rightComponent The component to display on the right side.
     * @param gbc GridBagConstraints to use.
     */
    protected static void addLine(final JPanel panel, final JComponent leftComponent, final JPanel rightComponent,
        final GridBagConstraints gbc) {

        eliminateHGapIfPossible(leftComponent);

        gbc.anchor = GridBagConstraints.WEST;
        gbc.fill = GridBagConstraints.NONE;
        gbc.insets = new Insets(5, 15, 0, 0);
        gbc.gridx = 0;
        gbc.weightx = 0.0;
        gbc.weighty = 0.0;
        panel.add(leftComponent, gbc);

        eliminateHGapIfPossible(rightComponent);
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.gridx = 1;
        gbc.insets = new Insets(5, 15, 0, 0);
        panel.add(rightComponent, gbc);

        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.gridx = 2;
        gbc.weightx = 1;
        panel.add(Box.createHorizontalGlue(), gbc);
        gbc.fill = GridBagConstraints.NONE;
        gbc.gridx = 0;
        gbc.weightx = 0;
        gbc.insets = new Insets(5, 5, 5, 5);
    }

    /**
     * @return the mode of the underlying settings class.
     */
    public DecisionTreeLearnerMode getMode() {
        return m_settings.getMode();
    }

    /**
     * @return the underlying {@link DecisionTreeComponents}
     */
    protected COMPONENTS getComponents() {
        return m_components;
    }

    /**
     * @return the underlying settings class
     */
    protected SETTINGS getSettings() {
        return m_settings;
    }

    /**
     * @param component
     */
    protected static void eliminateHGapIfPossible(final JComponent component) {
        final LayoutManager leftLayoutManager = component.getLayout();
        if (leftLayoutManager != null && leftLayoutManager instanceof FlowLayout) {
            ((FlowLayout)leftLayoutManager).setAlignment(FlowLayout.LEFT);
            ((FlowLayout)leftLayoutManager).setHgap(0);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] ports)
        throws NotConfigurableException {
        final DataTableSpec[] tableSpecs = MLlibNodeSettings.getTableSpecInDialog(0, ports);
        m_components.loadSettingsFrom(settings, tableSpecs[0]);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_components.saveSettingsTo(settings);
    }
}
