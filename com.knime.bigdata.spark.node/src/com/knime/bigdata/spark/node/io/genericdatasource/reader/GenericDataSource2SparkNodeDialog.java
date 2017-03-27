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
 *   Created on Aug 9, 2016 by sascha
 */
package com.knime.bigdata.spark.node.io.genericdatasource.reader;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import javax.swing.DefaultComboBoxModel;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;

import org.knime.base.filehandling.NodeUtils;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.base.filehandling.remote.dialog.RemoteFileChooser;
import org.knime.base.filehandling.remote.dialog.RemoteFileChooserPanel;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.FlowVariableModel;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.util.StringHistory;
import org.knime.core.node.workflow.FlowVariable;

import com.knime.bigdata.spark.core.context.SparkContextID;
import com.knime.bigdata.spark.core.node.SparkSourceNodeModel;
import com.knime.bigdata.spark.core.port.data.SparkDataPreviewPanel;
import com.knime.bigdata.spark.core.port.data.SparkDataTable;

/**
 * @author Sascha Wolke, KNIME.com
 * @param <T> Settings type.
 */
public class GenericDataSource2SparkNodeDialog<T extends GenericDataSource2SparkSettings> extends NodeDialogPane {

    /** Internal settings model */
    protected final T m_settings;

    private final FlowVariableModel m_filenameFlowVariable;
    private final RemoteFileChooserPanel m_filenameChooser;

    private final JCheckBox m_uploadDriver;

    /** Main option panel. */
    protected final JPanel m_optionsPanel;

    /** Grid bag constraints used on main options panel. */
    protected final GridBagConstraints m_optionsPanelConstraints;

    private final SparkDataPreviewPanel m_previewPanel;

    private SparkContextID m_contextId;

    /**
     * Default constructor.
     * @see #addToOptionsPanel(String, JComponent) for customization
     * @param initialSettings - Initial settings object
     */
    public GenericDataSource2SparkNodeDialog(final T initialSettings) {
        m_settings = initialSettings;

        m_optionsPanel = new JPanel(new GridBagLayout());
        m_optionsPanelConstraints = new GridBagConstraints();
        NodeUtils.resetGBC(m_optionsPanelConstraints);

        m_filenameFlowVariable = createFlowVariableModel(GenericDataSource2SparkSettings.CFG_INPUT_PATH, FlowVariable.Type.STRING);
        m_filenameChooser = new RemoteFileChooserPanel(getPanel(), "Source file", false,
            "inputNameSpark_" + m_settings.getFormat(),
            RemoteFileChooser.SELECT_FILE_OR_DIR, m_filenameFlowVariable, null);
        addToOptionsPanel("Source", m_filenameChooser.getPanel());

        if (m_settings.hasDriver()) {
            m_uploadDriver = new JCheckBox("Upload data source driver");
            addToOptionsPanel("Driver", m_uploadDriver);
        } else {
            m_uploadDriver = null;
        }

        addTab("Options", m_optionsPanel);

        m_previewPanel = new SparkDataPreviewPanel() {
            private static final long serialVersionUID = -8340686067208065014L;

            @Override
            protected SparkDataTable prepareDataTable(final ExecutionMonitor exec) throws Exception {
                final NodeSettings tmpSettings = new NodeSettings("preview");
                saveSettingsTo(tmpSettings);
                final GenericDataSource2SparkSettings prevSettings = m_settings.newInstance();
                prevSettings.loadSettings(tmpSettings);
                return GenericDataSource2SparkNodeModel.preparePreview(prevSettings, m_contextId, exec);
            }
        };
        addTab("Preview", m_previewPanel, false);
    }

    /**
     * Add components with description to options panel.
     * @param description - Text on the left
     * @param component - Component on the right
     */
    protected void addToOptionsPanel(final String description, final JComponent component) {
        m_optionsPanelConstraints.gridx = 0;
        m_optionsPanelConstraints.gridy++;
        m_optionsPanelConstraints.weightx = 0;
        m_optionsPanel.add(new JLabel(description + ": "), m_optionsPanelConstraints);
        m_optionsPanelConstraints.gridx++;
        m_optionsPanelConstraints.weightx = 1;
        m_optionsPanel.add(component, m_optionsPanelConstraints);
    }

    /**
     * Add second component to options panel (after first component with description added).
     * @param component - Component on the right
     */
    protected void addToOptionsPanel(final JComponent component) {
        m_optionsPanelConstraints.gridy++;
        m_optionsPanelConstraints.weightx = 1;
        m_optionsPanel.add(component, m_optionsPanelConstraints);
    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
            throws NotConfigurableException {

        ConnectionInformation connInfo = null;
        if (specs.length > 0 && specs[0] != null) {
            connInfo = ((ConnectionInformationPortObjectSpec) specs[0]).getConnectionInformation();
        }

        m_settings.loadSettings(settings);
        m_filenameChooser.setConnectionInformation(connInfo);
        m_filenameChooser.setSelection(m_settings.getInputPath());

        if (m_settings.hasDriver()) {
            m_uploadDriver.setSelected(m_settings.uploadDriver());
        }

        m_contextId = SparkSourceNodeModel.getContextID(specs);
        m_previewPanel.reset();
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_settings.setInputPath(m_filenameChooser.getSelection());

        if (m_settings.hasDriver()) {
            m_settings.setUploadDriver(m_uploadDriver.isSelected());
        }

        m_settings.validateSettings();
        m_settings.saveSettingsTo(settings);
    }

    /**
     * @param comboBox - Combo box with strings
     * @return Current selection
     */
    protected String getSelection(final JComboBox<String> comboBox) {
        return comboBox.getEditor().getItem().toString();
    }

    /**
     * Update a combo box with string history.
     * @param id - History ID
     * @param comboBox - Combo box with strings
     */
    protected void updateHistory(final String id, final JComboBox<String> comboBox) {
        final StringHistory history = StringHistory.getInstance(id);
        final Set<String> set = new LinkedHashSet<>();
        Collections.addAll(set, history.getHistory());
        final DefaultComboBoxModel<String> model = (DefaultComboBoxModel<String>) comboBox.getModel();
        model.removeAllElements();
        for (final String string : set) {
            model.addElement(string);
        }
    }
}
