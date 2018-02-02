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
package org.knime.bigdata.spark.local.node.create;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JFileChooser;
import javax.swing.JPanel;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.bigdata.spark.local.node.create.LocalSparkContextSettings.SQLSupport;
import org.knime.core.node.FlowVariableModel;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentButtonGroup;
import org.knime.core.node.defaultnodesettings.DialogComponentMultiLineString;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.util.FilesHistoryPanel;
import org.knime.core.node.util.FilesHistoryPanel.LocationValidation;
import org.knime.core.node.workflow.FlowVariable;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
class LocalEnvironmentCreatorNodeDialog extends NodeDialogPane implements ChangeListener {

    private LocalSparkContextSettings m_settings = new LocalSparkContextSettings();

    private final FlowVariableModel m_hiveFolderFlowVariable;

    private final FilesHistoryPanel m_hiveFolderChooser;
    
    /**
     * Constructor.
     */
    LocalEnvironmentCreatorNodeDialog() {
        JPanel panel = new JPanel(new GridBagLayout());
        GridBagConstraints c = new GridBagConstraints();
        c.insets = new Insets(5, 5, 5, 5);
        c.anchor = GridBagConstraints.NORTHWEST;
        
        c.gridx = 0;
        c.gridy++;
        panel.add(new DialogComponentString(m_settings.getContextNameModel(), "Context name: ", true, 30).getComponentPanel(),
            c);
        
        c.gridx = 0;
        c.gridy++;
        panel.add(new DialogComponentNumber(m_settings.getNumberOfThreadsModel(), "Number of threads: ", 1, 5)
            .getComponentPanel(), c);
        
        c.gridx = 0;
        c.gridy++;
        panel.add(new DialogComponentButtonGroup(m_settings.getOnDisposeActionModel(),
            "Action to perform on dispose", true, LocalSparkContextSettings.OnDisposeAction.values()).getComponentPanel(), c);
        
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
        panel.add(new DialogComponentButtonGroup(m_settings.getSqlSupportModel(),
            "SQL Support", true, LocalSparkContextSettings.SQLSupport.values()).getComponentPanel(), c);
        m_settings.getSqlSupportModel().addChangeListener(this);
        
        c.gridx = 0;
        c.gridy++;
        panel.add(new DialogComponentBoolean(m_settings.getUseHiveDataFolderModel(), "Use custom Hive data folder (Metastore DB & Warehouse)")
            .getComponentPanel(), c);
        m_settings.getUseHiveDataFolderModel().addChangeListener(this);

        c.gridx = 0;
        c.gridy++;
        m_hiveFolderFlowVariable = createFlowVariableModel(m_settings.getHiveDataFolderModel().getKey(), FlowVariable.Type.STRING);
        m_hiveFolderChooser = new FilesHistoryPanel(m_hiveFolderFlowVariable,
                    "local_spark_hive_data_folder",
                    LocationValidation.DirectoryOutput);
        m_hiveFolderChooser.setDialogType(JFileChooser.OPEN_DIALOG);
        final JPanel hiveFolderChooserPanel = new JPanel();
        hiveFolderChooserPanel.setLayout(new BoxLayout(hiveFolderChooserPanel, BoxLayout.X_AXIS));
        hiveFolderChooserPanel.setMaximumSize(new Dimension(Integer.MAX_VALUE, m_hiveFolderChooser.getPreferredSize().height));
        hiveFolderChooserPanel.add(m_hiveFolderChooser);
        hiveFolderChooserPanel.add(Box.createHorizontalGlue());
        panel.add(hiveFolderChooserPanel, c);

        
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
        } else if (eventSource == m_settings.getSqlSupportModel() || eventSource == m_settings.getUseHiveDataFolderModel()) {
        	final boolean hiveEnabled = SQLSupport.valueOf(m_settings.getSqlSupportModel().getStringValue()) != SQLSupport.SPARK_SQL_ONLY;
        	m_settings.getUseHiveDataFolderModel().setEnabled(hiveEnabled);
        	
        	final boolean hiveDataFolderChooserEnabled = hiveEnabled && m_settings.useHiveDataFolder();
        	m_settings.getHiveDataFolderModel().setEnabled(hiveDataFolderChooserEnabled);
        	m_hiveFolderChooser.setEnabled(hiveDataFolderChooserEnabled);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_settings.getHiveDataFolderModel().setStringValue(m_hiveFolderChooser.getSelectedFile());
        m_settings.validateDeeper();
        m_settings.saveSettingsTo(settings);
    }


    private void updateHiveSettingsEnabledness() {
    	
    	
    	
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs) throws NotConfigurableException {
        try {
            m_settings.loadSettingsFrom(settings);
            m_hiveFolderChooser.setSelectedFile(m_settings.getHiveDataFolderModel().getStringValue());
            updateHiveSettingsEnabledness();
            
        } catch (InvalidSettingsException e) {
            throw new NotConfigurableException(e.getMessage());
        }
    }
}