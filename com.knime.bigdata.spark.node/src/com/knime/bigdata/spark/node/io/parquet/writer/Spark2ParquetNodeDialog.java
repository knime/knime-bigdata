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
 *   Created on Aug 10, 2016 by sascha
 */
package com.knime.bigdata.spark.node.io.parquet.writer;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.LinkedHashSet;
import java.util.Set;

import javax.swing.DefaultComboBoxModel;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.base.filehandling.NodeUtils;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.base.filehandling.remote.dialog.RemoteFileChooser;
import org.knime.base.filehandling.remote.dialog.RemoteFileChooserPanel;
import org.knime.core.node.FlowVariableModel;
import org.knime.core.node.FlowVariableModelButton;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.util.StringHistory;
import org.knime.core.node.workflow.FlowVariable;

import com.knime.bigdata.spark.node.SparkSaveMode;

/**
 * @author Sascha Wolke, KNIME.com
 */
public class Spark2ParquetNodeDialog extends NodeDialogPane {

    private final Spark2ParquetSettings m_settings = new Spark2ParquetSettings();

    private final JLabel m_fsInfo;

    private final FlowVariableModel m_directoryFlowVariable;
    private final RemoteFileChooserPanel m_directoryChooser;

    private final JComboBox<String> m_outputNameComboBox;
    private final FlowVariableModel m_outputNameFlowVariable;
    private final FlowVariableModelButton m_outputNameFlowVariableButton;

    private final JComboBox<SparkSaveMode> m_saveModeComboBox;

    Spark2ParquetNodeDialog() {
        m_fsInfo = new JLabel();
        m_directoryFlowVariable = createFlowVariableModel("targetDir", FlowVariable.Type.STRING);
        m_directoryChooser = new RemoteFileChooserPanel(getPanel(), "Target dir", false,
            "directoryHistory", RemoteFileChooser.SELECT_DIR, m_directoryFlowVariable, null);
        m_outputNameComboBox = new JComboBox<String>();
        m_outputNameComboBox.setEditable(true);
        m_outputNameFlowVariable = createFlowVariableModel("targetFilename", FlowVariable.Type.STRING);
        m_outputNameFlowVariable.addChangeListener( new ChangeListener() {
            @Override
            public void stateChanged(final ChangeEvent e) {
                m_outputNameComboBox.setEnabled(!m_outputNameFlowVariable.isVariableReplacementEnabled());
            }
        });
        m_outputNameFlowVariableButton = new FlowVariableModelButton(m_outputNameFlowVariable);

        m_saveModeComboBox = new JComboBox<SparkSaveMode>(SparkSaveMode.ALL);
        m_saveModeComboBox.setEditable(false);

        addTab("Options", initLayout());
    }

    private JPanel initLayout() {
        JPanel panel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        NodeUtils.resetGBC(gbc);

        panel.add(new JLabel("Using filesystem: "), gbc);
        gbc.gridx++;
        gbc.weightx = 1;
        panel.add(filesystemInfoPanel(), gbc);

        gbc.gridx = 0;
        gbc.gridy++;
        gbc.weightx = 0;
        panel.add(new JLabel("Output dir: "), gbc);
        gbc.gridx++;
        gbc.weightx = 1;
        panel.add(m_directoryChooser.getPanel(), gbc);

        gbc.gridx = 0;
        gbc.gridy++;
        gbc.weightx = 0;
        panel.add(new JLabel("Output name: "), gbc);
        gbc.gridx++;
        gbc.weightx = 1;
        panel.add(outputNamePanel(), gbc);

        gbc.gridx = 0;
        gbc.gridy++;
        panel.add(new JLabel("Save mode: "), gbc);
        gbc.gridx++;
        gbc.weightx = 2;
        panel.add(saveModePanel(), gbc);

        return panel;
    }

    private JPanel filesystemInfoPanel() {
        JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        NodeUtils.resetGBC(gbc);
        gbc.insets = new Insets(5, 5, 5, 5);
        gbc.anchor = GridBagConstraints.WEST;
        panel.add(m_fsInfo, gbc);
        return panel;
    }

    private JPanel outputNamePanel() {
        JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        NodeUtils.resetGBC(gbc);
        gbc.insets = new Insets(5, 5, 5, 0);
        gbc.weightx = 1;
        panel.add(m_outputNameComboBox, gbc);
        gbc.weightx = 0;
        gbc.fill = GridBagConstraints.NONE;
        gbc.gridx++;
        gbc.insets = new Insets(5, 5, 5, 5);
        panel.add(m_outputNameFlowVariableButton, gbc);
        return panel;
    }

    private JPanel saveModePanel() {
        JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        NodeUtils.resetGBC(gbc);
        gbc.insets = new Insets(5, 5, 5, 5);
        gbc.anchor = GridBagConstraints.WEST;
        panel.add(m_saveModeComboBox, gbc);
        return panel;
    }

    private String getFilenameSelection() {
        return (String) m_outputNameComboBox.getEditor().getItem();
    }

    private SparkSaveMode getSaveModeSelection() {
        return (SparkSaveMode)m_saveModeComboBox.getSelectedItem();
    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
            throws NotConfigurableException {

        if (specs == null || specs.length < 1 || specs[0] == null) {
            throw new NotConfigurableException("HDFS connection information missing");
        }

        ConnectionInformationPortObjectSpec object = (ConnectionInformationPortObjectSpec) specs[0];
        ConnectionInformation connectionInformation = object.getConnectionInformation();
        if (connectionInformation == null) {
            throw new NotConfigurableException("HDFS connection information missing");
        }
        m_directoryChooser.setConnectionInformation(connectionInformation);
        m_fsInfo.setText(connectionInformation.toURI().toString());

        m_settings.loadSettings(settings);
        m_directoryChooser.setSelection(m_settings.getDirectory());

        updateHistory(m_outputNameComboBox, "filenameHistory");
        m_outputNameComboBox.setSelectedItem(m_settings.getTableName());
        m_saveModeComboBox.setSelectedItem(m_settings.getSaveMode());
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_settings.setDirectory(m_directoryChooser.getSelection());
        m_settings.setTableName(getFilenameSelection());
        m_settings.setSaveMode(getSaveModeSelection());

        m_settings.validateSettings();
        m_settings.saveSettingsTo(settings);
        updateHistory(m_outputNameComboBox, "filenameHistory");
    }

    /**
     * Update the history of the combo box.
     */
    private void updateHistory(final JComboBox<String> comboBox, final String historyID) {
        // Get history
        final StringHistory history = StringHistory.getInstance(historyID);
        // Get values
        final String[] strings = history.getHistory();
        // Make values unique through use of set
        final Set<String> set = new LinkedHashSet<String>();
        for (final String string : strings) {
            set.add(string);
        }
        // Remove old elements
        final DefaultComboBoxModel<String> model = (DefaultComboBoxModel<String>) comboBox.getModel();
        model.removeAllElements();
        // Add new elements
        for (final String string : set) {
            model.addElement(string);
        }
    }
}
