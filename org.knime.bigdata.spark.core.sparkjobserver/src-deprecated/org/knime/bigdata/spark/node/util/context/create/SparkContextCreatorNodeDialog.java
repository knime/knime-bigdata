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
 *   Created on 03.07.2015 by koetter
 */
package org.knime.bigdata.spark.node.util.context.create;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import javax.swing.JPanel;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.bigdata.spark.core.sparkjobserver.JobserverSparkContextProvider;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentAuthentication;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentMultiLineString;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication.AuthenticationType;
import org.knime.core.node.port.PortObjectSpec;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
class SparkContextCreatorNodeDialog extends NodeDialogPane implements ChangeListener {

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
        panel.add(new DialogComponentString(m_settings.getJobServerUrlModel(), "Jobserver URL: ", true, 30).getComponentPanel(), gbc);
        gbc.gridx = 0;
        gbc.gridy++;
        m_authenticationComp = new DialogComponentAuthentication(m_settings.getAuthenticateModel(), "Authentication",
                        AuthenticationType.NONE, AuthenticationType.CREDENTIALS, AuthenticationType.USER_PWD);
        panel.add(m_authenticationComp.getComponentPanel(), gbc);
        m_settings.getAuthenticateModel().addChangeListener(this);
        gbc.gridx = 0;
        gbc.gridy++;
        panel.add(new DialogComponentNumber(m_settings.getReceiveTimeoutModel(),
            "Jobserver response timeout (seconds): ", 10, 5).getComponentPanel(), gbc);
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
                "Spark version: ", getAllSparkVersionLabels()).getComponentPanel(), gbc);
        gbc.gridx = 0;
        gbc.gridy++;
        panel.add(new DialogComponentString(m_settings.getContextNameModel(), "Context name: ", true, 30).getComponentPanel(), gbc);
        gbc.gridx = 0;
        gbc.gridy++;
        panel.add(new DialogComponentBoolean(m_settings.getDeleteContextOnDisposeModel(),
                "<html>Destroy Spark context on dispose</html>").getComponentPanel(), gbc);
        m_settings.getDeleteContextOnDisposeModel().addChangeListener(this);
        gbc.gridx = 0;
        gbc.gridy++;
        panel.add(new DialogComponentBoolean(m_settings.getDeleteObjectsOnDisposeModel(),
                "<html>Delete Spark DataFrames/RDDs on dispose</html>").getComponentPanel(), gbc);
        gbc.gridx = 0;
        gbc.gridy++;
        panel.add(new DialogComponentBoolean(m_settings.getOverrideSparkSettingsModel(),
                "Override Spark settings").getComponentPanel(), gbc);
        m_settings.getOverrideSparkSettingsModel().addChangeListener(this);
        gbc.gridx = 0;
        gbc.gridy++;
        gbc.fill = GridBagConstraints.BOTH;
        panel.add(new DialogComponentMultiLineString(m_settings.getCustomSparkSettingsModel(),
                "Custom Spark settings: ", true, 40, 5).getComponentPanel(), gbc);

        gbc.gridx = 0;
        gbc.gridy++;
        gbc.fill = GridBagConstraints.NONE;
        panel.add(new DialogComponentBoolean(m_settings.getHideExistsWarningModel(),
                "Hide warning about an existing Spark context").getComponentPanel(), gbc);

        return panel;
    }

    /**
     * @return all supported spark version labels
     */
    private static String[] getAllSparkVersionLabels() {
        final SparkVersion[] versions = new JobserverSparkContextProvider().getSupportedSparkVersions().toArray(new SparkVersion[0]);
        final String[] labels = new String[versions.length];
        for (int i = 0; i < versions.length; i++) {
            labels[i] = versions[i].getLabel();
        }
        Arrays.sort(labels, Collections.reverseOrder());
        return labels;
    }

    @Override
    public void stateChanged(final ChangeEvent e) {
        if (e.getSource().equals(m_settings.getOverrideSparkSettingsModel())) {
            m_settings.getCustomSparkSettingsModel().setEnabled(m_settings.getOverrideSparkSettingsModel().getBooleanValue());
        }

        if (e.getSource().equals(m_settings.getDeleteContextOnDisposeModel())) {
            m_settings.getDeleteObjectsOnDisposeModel().setBooleanValue(m_settings.getDeleteContextOnDisposeModel().getBooleanValue());
            m_settings.getDeleteObjectsOnDisposeModel().setEnabled(!m_settings.getDeleteContextOnDisposeModel().getBooleanValue());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_settings.validateSettings();
        m_settings.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs) throws NotConfigurableException {
        try {
            m_authenticationComp.loadCredentials(getCredentialsProvider());
            m_settings.loadSettingsFrom(settings);
        } catch (InvalidSettingsException e) {
            throw new NotConfigurableException(e.getMessage());
        }
    }
}
