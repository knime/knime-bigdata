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
package com.knime.bigdata.spark.node.context;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.BorderFactory;
import javax.swing.JPanel;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentButtonGroup;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.defaultnodesettings.DialogComponentPasswordField;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.port.PortObjectSpec;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
class SparkContextCreatorNodeDialog extends NodeDialogPane {

    private ContextSettings m_settings = new ContextSettings();

    /**
     * Constructor.
     */
    SparkContextCreatorNodeDialog() {
        JPanel panel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.fill = GridBagConstraints.NONE;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.gridwidth = 1;
        gbc.gridheight = 1;
        gbc.weightx = 1;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        panel.add(createKNIMEPanel(), gbc);
        gbc.gridx = 0;
        gbc.gridy++;
        panel.add(createContextPanel(), gbc);
        gbc.gridx = 0;
        gbc.gridy++;
        panel.add(createJobServerPanel(), gbc);
        addTab("Settings", panel);
    }

    /**
     * @param panel
     * @param gbc
     * @return
     */
    private JPanel createJobServerPanel() {
        JPanel panel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.fill = GridBagConstraints.NONE;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.gridwidth = 1;
        gbc.gridheight = 1;
        panel.setBorder(BorderFactory.createTitledBorder(BorderFactory
            .createEtchedBorder(), " Job Server "));
        gbc.gridx = 0;
        gbc.gridy++;
        panel.add(new DialogComponentButtonGroup(m_settings.getProtocolModel(), false, " Protocol ",
            new String[] {"http", "https"}).getComponentPanel(), gbc);
        gbc.gridx = 0;
        gbc.gridy++;
        panel.add(new DialogComponentString(m_settings.getHostModel(), "Host: ").getComponentPanel(), gbc);
        gbc.gridx = 0;
        gbc.gridy++;
        panel.add(new DialogComponentNumber(m_settings.getPortModel(), "Port: ", 1).getComponentPanel(), gbc);
        gbc.gridx = 0;
        gbc.gridy++;
        panel.add(new DialogComponentString(m_settings.getUserModel(), "User: ").getComponentPanel(), gbc);
        gbc.gridx = 0;
        gbc.gridy++;
        panel.add(new DialogComponentPasswordField(m_settings.getPasswordModel(), "Password: ").getComponentPanel(), gbc);
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
        panel.setBorder(BorderFactory.createTitledBorder(BorderFactory
            .createEtchedBorder(), " Spark Context "));
        gbc.gridx = 0;
        gbc.gridy++;
        panel.add(new DialogComponentString(m_settings.getContextNameModel(), "Context name: ").getComponentPanel(), gbc);
        gbc.gridx = 0;
        gbc.gridy++;
        panel.add(new DialogComponentString(m_settings.getMemoryModel(), "Memory: ").getComponentPanel(), gbc);
        gbc.gridx = 0;
        gbc.gridy++;
        panel.add(new DialogComponentNumber(m_settings.getNoOfCoresModel(), "Number of cores: ", 1).getComponentPanel(), gbc);
        return panel;
    }

    private JPanel createKNIMEPanel() {
        JPanel panel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.fill = GridBagConstraints.NONE;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.gridwidth = 1;
        gbc.gridheight = 1;
        panel.setBorder(BorderFactory.createTitledBorder(BorderFactory
            .createEtchedBorder(), " KNIME "));
        panel.add(new DialogComponentNumber(m_settings.getJobTimeoutModel(),
            "Spark job timeout (seconds): ", 1).getComponentPanel(), gbc);
        gbc.gridx = 0;
        gbc.gridy++;
        panel.add(new DialogComponentNumber(m_settings.getJobCheckFrequencyModel(),
            "Spark job check frequency (seconds): ", 1).getComponentPanel(), gbc);
        gbc.gridx = 0;
        gbc.gridy++;
        panel.add(new DialogComponentBoolean(m_settings.getDeleteRDDsOnDisposeModel(),
                "Delete RDDs on dispose").getComponentPanel(), gbc);
        return panel;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_settings.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs) throws NotConfigurableException {
        try {
            m_settings.loadSettingsFrom(settings);
        } catch (InvalidSettingsException e) {
            throw new NotConfigurableException(e.getMessage());
        }
    }
}
