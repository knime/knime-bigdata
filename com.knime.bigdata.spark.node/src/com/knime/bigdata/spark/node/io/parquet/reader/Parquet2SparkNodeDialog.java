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
package com.knime.bigdata.spark.node.io.parquet.reader;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.JLabel;
import javax.swing.JPanel;

import org.knime.base.filehandling.NodeUtils;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.base.filehandling.remote.dialog.RemoteFileChooser;
import org.knime.base.filehandling.remote.dialog.RemoteFileChooserPanel;
import org.knime.core.node.FlowVariableModel;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.workflow.FlowVariable;

/**
 * @author Sascha Wolke, KNIME.com
 */
public class Parquet2SparkNodeDialog extends NodeDialogPane {

    private final Parquet2SparkSettings m_settings = new Parquet2SparkSettings();

    private final JLabel m_info;

    private final FlowVariableModel m_filenameFlowVariable;
    private final RemoteFileChooserPanel m_filenameChooser;

    public Parquet2SparkNodeDialog() {
        m_info = new JLabel();
        m_filenameFlowVariable = createFlowVariableModel("sourceFilename", FlowVariable.Type.STRING);
        m_filenameChooser = new RemoteFileChooserPanel(getPanel(), "Source file", false,
            "filenameHistory", RemoteFileChooser.SELECT_FILE_OR_DIR, m_filenameFlowVariable, null);

        addTab("Options", initLayout());
    }

    private JPanel initLayout() {
        JPanel panel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        NodeUtils.resetGBC(gbc);
        gbc.gridwidth = 2;
        panel.add(m_info, gbc);
        gbc.gridy++;
        gbc.gridwidth = 1;
        panel.add(new JLabel("Input table: "), gbc);
        gbc.gridx++;
        gbc.weightx = 2;
        panel.add(m_filenameChooser.getPanel(), gbc);
        return panel;
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
        m_filenameChooser.setConnectionInformation(connectionInformation);
        m_info.setText("Using filesystem: " + connectionInformation.toURI());

        m_settings.loadSettingsForDialog(settings);
        m_filenameChooser.setSelection(m_settings.getInputPath());
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_settings.setInputPath(m_filenameChooser.getSelection());
        m_settings.saveSettingsTo(settings);
    }
}
