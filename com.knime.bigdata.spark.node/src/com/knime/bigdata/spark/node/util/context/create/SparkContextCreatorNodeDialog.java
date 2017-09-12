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
 *   Created on 03.07.2015 by koetter
 */
package com.knime.bigdata.spark.node.util.context.create;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.JPanel;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentAuthentication;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentButtonGroup;
import org.knime.core.node.defaultnodesettings.DialogComponentMultiLineString;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication.AuthenticationType;
import org.knime.core.node.port.PortObjectSpec;

import com.knime.bigdata.spark.core.node.AbstractSparkNodeDialogPane;
import com.knime.bigdata.spark.core.preferences.SparkPreferenceInitializer;
import com.knime.bigdata.spark.core.version.SparkVersion;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
class SparkContextCreatorNodeDialog extends AbstractSparkNodeDialogPane implements ChangeListener {

    private ContextSettings m_settings = new ContextSettings();

    private DialogComponentAuthentication m_authenticationComp;

    /**
     * Constructor.
     */
    SparkContextCreatorNodeDialog() {
        addTab("Context Settings", createContextPanel());
        addTab("Connection Settings", createConnectionPanel());
    }


    private JPanel createConnectionPanel() {
        JPanel panel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.fill = GridBagConstraints.NONE;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.gridwidth = 1;
        gbc.gridheight = 1;
        gbc.gridx = 0;
        gbc.gridy++;
        panel.add(new DialogComponentString(m_settings.getJobServerUrlModel(), "Job server URL: ", true, 30).getComponentPanel(), gbc);
        gbc.gridx = 0;
        gbc.gridy++;
        m_authenticationComp = new DialogComponentAuthentication(m_settings.getAuthenticateModel(), "Authentication",
                        AuthenticationType.NONE, AuthenticationType.CREDENTIALS, AuthenticationType.USER_PWD);
        panel.add(m_authenticationComp.getComponentPanel(), gbc);
        m_settings.getAuthenticateModel().addChangeListener(this);
        gbc.gridx = 0;
        gbc.gridy++;
        panel.add(new DialogComponentNumber(m_settings.getJobTimeoutModel(),
            "Spark job timeout (seconds): ", 1, 5).getComponentPanel(), gbc);
        gbc.gridx = 0;
        gbc.gridy++;
        panel.add(new DialogComponentNumber(m_settings.getJobCheckFrequencyModel(),
            "Spark job check frequency (seconds): ", 1, 5).getComponentPanel(), gbc);

        return panel;
    }

    private JPanel createContextPanel() {
        JPanel panel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.fill = GridBagConstraints.NONE;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.gridwidth = 1;
        gbc.gridheight = 1;
        panel.add(new DialogComponentStringSelection(m_settings.getSparkVersionModel(),
                "Spark version: ", SparkVersion.getAllVersionLabels()).getComponentPanel(), gbc);
        gbc.gridx = 0;
        gbc.gridy++;
        panel.add(new DialogComponentString(m_settings.getContextNameModel(), "Context name: ", true, 30).getComponentPanel(), gbc);
        gbc.gridx = 0;
        gbc.gridy++;
        panel.add(new DialogComponentBoolean(m_settings.getDeleteObjectsOnDisposeModel(),
                "Delete objects on dispose").getComponentPanel(), gbc);
        gbc.gridx = 0;
        gbc.gridy++;
        panel.add(new DialogComponentButtonGroup(m_settings.getSparkJobLogLevelModel(), false,
                "Spark job log level:", SparkPreferenceInitializer.ALL_LOG_LEVELS).getComponentPanel(), gbc);
        gbc.gridx = 0;
        gbc.gridy++;
        panel.add(new DialogComponentBoolean(m_settings.getOverrideSparkSettingsModel(),
                "Override spark settings").getComponentPanel(), gbc);
        m_settings.getOverrideSparkSettingsModel().addChangeListener(this);
        gbc.gridx = 0;
        gbc.gridy++;
        gbc.fill = GridBagConstraints.BOTH;
        panel.add(new DialogComponentMultiLineString(m_settings.getCustomSparkSettingsModel(),
                "Custom spark settings: ", true, 40, 5).getComponentPanel(), gbc);

        gbc.gridx = 0;
        gbc.gridy++;
        gbc.fill = GridBagConstraints.NONE;
        panel.add(new DialogComponentBoolean(m_settings.getHideExistsWarningModel(),
                "Hide context exists warning").getComponentPanel(), gbc);

        return panel;
    }

    @Override
    public void stateChanged(final ChangeEvent e) {
        if (e.getSource().equals(m_settings.getOverrideSparkSettingsModel())) {
            m_settings.getCustomSparkSettingsModel().setEnabled(m_settings.getOverrideSparkSettingsModel().getBooleanValue());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveAdditionalSparkSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_settings.validateSettings();
        m_settings.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadAdditionalSparkSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs) throws NotConfigurableException {
        try {
            m_authenticationComp.loadCredentials(getCredentialsProvider());
            m_settings.loadSettingsFrom(settings);
        } catch (InvalidSettingsException e) {
            throw new NotConfigurableException(e.getMessage());
        }
    }
}
