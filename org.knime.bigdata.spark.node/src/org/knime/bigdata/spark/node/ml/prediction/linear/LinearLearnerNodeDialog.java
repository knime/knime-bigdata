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
 */
package org.knime.bigdata.spark.node.ml.prediction.linear;

import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.LayoutManager;

import javax.swing.Box;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JSeparator;

import org.knime.bigdata.spark.core.node.MLlibNodeSettings;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;

/**
 * Dialog for ml-based linear learners nodes.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class LinearLearnerNodeDialog extends NodeDialogPane {

    private final LinearLearnerSettings m_settings;

    private final LinearLearnerComponents m_components;

    private DataTableSpec m_tableSpec;

    /**
     * Default constructor.
     *
     * @param settings the settings model to use
     */
    public LinearLearnerNodeDialog(final LinearLearnerSettings settings) {
        m_settings = settings;
        m_components = new LinearLearnerComponents(settings);
        addTab("Settings", getSettingsTab());
        addTab("Advanced", getAdvancedTab());
    }

    private JPanel getAdvancedTab() {
        final JPanel tab = new JPanel(new GridBagLayout());

        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(5, 5, 5, 5);
        gbc.anchor = GridBagConstraints.NORTHWEST;
        gbc.fill = GridBagConstraints.NONE;
        gbc.gridx = 0;
        gbc.gridy = 0;

        addAdvancedOptions(tab, gbc);
        addVerticalGlue(tab, gbc);

        return tab;
    }

    private JPanel getSettingsTab() {
        final JPanel settingsTab = new JPanel(new GridBagLayout());

        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(5, 5, 5, 5);
        gbc.anchor = GridBagConstraints.NORTHWEST;
        gbc.fill = GridBagConstraints.NONE;
        gbc.gridx = 0;
        gbc.gridy = 0;

        addColumnSelections(settingsTab, gbc);

        addSeparatorAndLabel(settingsTab, "Regression Options", gbc);
        addRegressionOptions(settingsTab, gbc);

        addSeparatorAndLabel(settingsTab, "Regularization Options", gbc);
        addRegularizationOptions(settingsTab, gbc);

        addSeparatorAndLabel(settingsTab, "Missing Values in Input Columns", gbc);
        addMissingValuesOptions(settingsTab, gbc);

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

    private void addRegressionOptions(final JPanel panel, final GridBagConstraints gbc) {
        if (m_settings.getMode() == LinearLearnerMode.LINEAR_REGRESSION) {
            gbc.gridy++;
            addLine(panel, "Loss function", m_components.getLossFunctionComponent().getComponentPanel(), gbc);
        }

        gbc.gridy++;
        addLine(panel, "Standardize features", m_components.getStandardizationComponent().getComponentPanel(), gbc);
    }

    private void addRegularizationOptions(final JPanel panel, final GridBagConstraints gbc) {
        gbc.gridy++;
        addLine(panel, "Regularizer", m_components.getRegularizerComponent().getComponentPanel(), gbc);

        gbc.gridy++;
        addLine(panel, "Regularization parameter", m_components.getRegParamComponent().getComponentPanel(), gbc);

        gbc.gridy++;
        addLine(panel, "Elastic net parameter", m_components.getElasticNetParamComponent().getComponentPanel(), gbc);
    }

    private void addMissingValuesOptions(final JPanel panel, final GridBagConstraints gbc) {
        gbc.gridy++;
        addLine(panel, "Rows with missing values", m_components.getHandleInvalidComponent().getComponentPanel(), gbc);
    }

    private void addAdvancedOptions(final JPanel panel, final GridBagConstraints gbc) {
        if (m_settings.getMode() == LinearLearnerMode.LINEAR_REGRESSION) {
            gbc.gridy++;
            addLine(panel, "Solver", m_components.getSolverComponent().getComponentPanel(), gbc);
        } else if (m_settings.getMode() == LinearLearnerMode.LOGISTIC_REGRESSION) {
            gbc.gridy++;
            addLine(panel, "Family", m_components.getFamilyComponent().getComponentPanel(), gbc);
        }

        gbc.gridy++;
        addLine(panel, "Maximum iterations", m_components.getMaxIterComponent().getComponentPanel(), gbc);

        gbc.gridy++;
        addLine(panel, "Convergence tolerance", m_components.getConvergenceToleranceComponent().getComponentPanel(), gbc);

        gbc.gridy++;
        addLine(panel, "Fit intercept", m_components.getFitInterceptComponent().getComponentPanel(), gbc);
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

        // options label
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
        m_tableSpec = tableSpecs[0];
        m_components.loadSettingsFrom(settings, m_tableSpec);
        m_settings.updateEnabledness();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_settings.check(m_tableSpec);
        m_components.saveSettingsTo(settings);
    }
}
