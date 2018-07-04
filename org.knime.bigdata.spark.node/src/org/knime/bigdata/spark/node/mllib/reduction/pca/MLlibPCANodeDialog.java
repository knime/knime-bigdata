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
package org.knime.bigdata.spark.node.mllib.reduction.pca;

import static org.knime.bigdata.spark.node.mllib.reduction.pca.MLlibPCASettings.TARGET_DIM_MODE_FIXED;
import static org.knime.bigdata.spark.node.mllib.reduction.pca.MLlibPCASettings.TARGET_DIM_MODE_QUALITY;

import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.node.MLlibNodeSettings;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentButtonGroup;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnFilter2;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.port.PortObjectSpec;

/**
 * Spark PCA node dialog.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class MLlibPCANodeDialog extends NodeDialogPane implements ChangeListener {
    private final MLlibPCASettings m_settings;

    private final DialogComponentBoolean m_failOnMissingValues;

    private final DialogComponentButtonGroup m_targetDimMode;

    private final DialogComponentNumber m_noOfComponents;

    private final DialogComponentNumber m_minQuality;

    private final DialogComponentBoolean m_replaceInputColumns;

    private final DialogComponentColumnFilter2 m_featureColumns;

    private final boolean m_legacyMode;

    private DataTableSpec m_inSpec;

    private boolean m_supportsMinQuality;

    /**
     * Default constructor.
     *
     * @param legacyMode <code>true</code> if we should only present a column selection and number of components dialog
     */
    public MLlibPCANodeDialog(final boolean legacyMode) {
        m_legacyMode = legacyMode;
        m_settings = new MLlibPCASettings(legacyMode);
        m_failOnMissingValues = new DialogComponentBoolean(m_settings.getFailOnMissingValuesModel(),
            "Fail if missing values are encountered (skipped per default)");

        m_targetDimMode = new DialogComponentButtonGroup(m_settings.getTargetDimModeModel(), null, true,
            new String[]{"Dimensions to reduce to", "Minimum information fraction to preserve (%)"},
            new String[]{TARGET_DIM_MODE_FIXED, TARGET_DIM_MODE_QUALITY});

        m_noOfComponents = new DialogComponentNumber(m_settings.getNoOfComponentsModel(), "", 1, 10);
        m_minQuality = new DialogComponentNumber(m_settings.getMinQualityModel(), "", 1, 9);
        m_replaceInputColumns =
            new DialogComponentBoolean(m_settings.getReplaceInputColumnsModel(), "Replace original data columns");
        m_featureColumns = new DialogComponentColumnFilter2(m_settings.getFeatureColumnsModel(), 0);

        JPanel panel = new JPanel();
        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));
        if (!legacyMode) {
            panel.add(m_failOnMissingValues.getComponentPanel());
        }
        panel.add(createTargetDimensionPanel());
        if (!legacyMode) {
            panel.add(m_replaceInputColumns.getComponentPanel());
        }
        panel.add(m_featureColumns.getComponentPanel());
        addTab("Settings", panel);
        m_settings.getTargetDimModeModel().addChangeListener(this);
    }

    private JPanel createTargetDimensionPanel() {
        final JPanel panel = new JPanel(new FlowLayout());
        panel.setBorder(BorderFactory.createTitledBorder("Target dimensions"));
        panel.setLayout(new FlowLayout());

        if (m_legacyMode) {
            panel.add(new JLabel("Dimensions to reduce to"));
            panel.add(m_noOfComponents.getComponentPanel());

        } else {
            final JPanel subPanel = new JPanel(new GridBagLayout());
            final GridBagConstraints gbc = new GridBagConstraints();
            gbc.gridx = 0;
            gbc.gridy = 0;
            gbc.anchor = GridBagConstraints.WEST;
            gbc.fill = GridBagConstraints.HORIZONTAL;

            subPanel.add(m_targetDimMode.getButton(TARGET_DIM_MODE_FIXED), gbc);
            gbc.gridx++;
            subPanel.add(m_noOfComponents.getComponentPanel(), gbc);
            gbc.gridy++;
            gbc.gridx = 0;
            subPanel.add(m_targetDimMode.getButton(TARGET_DIM_MODE_QUALITY), gbc);
            gbc.gridx++;
            subPanel.add(m_minQuality.getComponentPanel(), gbc);

            panel.add(subPanel);
        }
        return panel;
    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {
        if (specs == null || specs.length < 1 || specs[0] == null) {
            throw new NotConfigurableException("Spark input data spec required.");
        }

        final SparkDataPortObjectSpec spec = (SparkDataPortObjectSpec)specs[0];
        final SparkVersion sparkVersion = SparkContextUtil.getSparkVersion(spec.getContextID());
        final DataTableSpec[] tableSpecs = MLlibNodeSettings.getTableSpecInDialog(0, specs);
        m_inSpec = tableSpecs[0];
        m_noOfComponents.loadSettingsFrom(settings, tableSpecs);
        m_featureColumns.loadSettingsFrom(settings, tableSpecs);

        if (!m_legacyMode) {
            m_failOnMissingValues.loadSettingsFrom(settings, tableSpecs);
            m_targetDimMode.loadSettingsFrom(settings, tableSpecs);
            m_minQuality.loadSettingsFrom(settings, tableSpecs);
            m_replaceInputColumns.loadSettingsFrom(settings, tableSpecs);

            if (SparkVersion.V_2_0.compareTo(sparkVersion) > 0) {
                m_settings.getTargetDimModeModel().setStringValue(TARGET_DIM_MODE_FIXED);
                m_targetDimMode.getButton(TARGET_DIM_MODE_QUALITY).setEnabled(false);
                m_supportsMinQuality = false;
            } else {
                m_targetDimMode.getButton(TARGET_DIM_MODE_QUALITY).setEnabled(true);
                m_supportsMinQuality = true;
            }
        }
        updateTargetDimState();
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_settings.validateSettings(m_inSpec);

        m_noOfComponents.saveSettingsTo(settings);
        m_featureColumns.saveSettingsTo(settings);

        if (!m_legacyMode) {
            m_failOnMissingValues.saveSettingsTo(settings);
            m_targetDimMode.saveSettingsTo(settings);
            m_minQuality.saveSettingsTo(settings);
            m_replaceInputColumns.saveSettingsTo(settings);
        }
    }

    private void updateTargetDimState() {
        if (m_settings.getTargetDimMode().equals(TARGET_DIM_MODE_FIXED) || !m_supportsMinQuality) {
            m_settings.getNoOfComponentsModel().setEnabled(true);
            m_settings.getMinQualityModel().setEnabled(false);
        } else {
            m_settings.getNoOfComponentsModel().setEnabled(false);
            m_settings.getMinQualityModel().setEnabled(true);
        }
    }

    @Override
    public void stateChanged(final ChangeEvent e) {
        updateTargetDimState();
    }
}
