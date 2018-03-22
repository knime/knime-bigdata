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
package org.knime.bigdata.spark.core.livy.node.create;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import javax.swing.JPanel;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentButtonGroup;
import org.knime.core.node.defaultnodesettings.DialogComponentMultiLineString;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.port.PortObjectSpec;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
class LivySparkContextCreatorNodeDialog extends NodeDialogPane implements ChangeListener {

    private LivySparkContextSettings m_settings = new LivySparkContextSettings();

    /**
     * Constructor.
     */
    LivySparkContextCreatorNodeDialog() {
        JPanel panel = new JPanel(new GridBagLayout());
        GridBagConstraints c = new GridBagConstraints();
        c.insets = new Insets(5, 5, 5, 5);
        c.anchor = GridBagConstraints.NORTHWEST;
        
        c.gridx = 0;
        c.gridy++;
        panel.add(new DialogComponentString(m_settings.getLivyUrlModel(), "Livy URL: ", true, 50).getComponentPanel(),
            c);
        
        c.gridx = 0;
        c.gridy++;
        panel.add(new DialogComponentString(m_settings.getContextNameModel(), "Context name: ", true, 30).getComponentPanel(),
            c);
        
        c.gridx = 0;
        c.gridy++;
        panel.add(new DialogComponentButtonGroup(m_settings.getOnDisposeActionModel(),
            "Action to perform on dispose", true, LivySparkContextSettings.OnDisposeAction.values()).getComponentPanel(), c);
        
        c.gridx = 0;
        c.gridy++;
        panel.add(new DialogComponentBoolean(m_settings.getUseCustomSparkSettingsModel(), "Use custom Spark settings")
            .getComponentPanel(), c);
        m_settings.getUseCustomSparkSettingsModel().addChangeListener(this);
        
        c.gridx = 0;
        c.gridy++;
        c.fill = GridBagConstraints.BOTH;
        panel.add(new DialogComponentMultiLineString(m_settings.getCustomSparkSettingsModel(),
            "Custom Spark settings: ", true, 40, 5).getComponentPanel(), c);
        
        c.gridx = 0;
        c.gridy++;
        c.fill = GridBagConstraints.NONE;
        panel.add(new DialogComponentBoolean(m_settings.getHideExistsWarningModel(),
                "Hide warning about an existing local Spark context").getComponentPanel(), c);

        addTab("Local Big Data Environment Settings", panel);
    }

    @Override
    public void stateChanged(final ChangeEvent e) {
    	final Object eventSource = e.getSource(); 
    	
    	if (eventSource == m_settings.getUseCustomSparkSettingsModel()) {
            m_settings.getCustomSparkSettingsModel()
                .setEnabled(m_settings.getUseCustomSparkSettingsModel().getBooleanValue());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_settings.validateDeeper();
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