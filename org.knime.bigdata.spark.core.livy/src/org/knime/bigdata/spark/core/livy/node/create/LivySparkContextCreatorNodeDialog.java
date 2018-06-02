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

import java.awt.CardLayout;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.swing.BorderFactory;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JSeparator;
import javax.swing.SwingConstants;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.bigdata.spark.core.livy.LivySparkContextProvider;
import org.knime.bigdata.spark.core.livy.node.create.LivySparkContextCreatorNodeSettings.ExecutorAllocation;
import org.knime.bigdata.spark.core.livy.node.create.ui.DialogComponentKeyValueEdit;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.defaultnodesettings.DialogComponentAuthentication;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentButtonGroup;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication.AuthenticationType;
import org.knime.core.node.port.PortObjectSpec;

/**
 * Node dialog of the "Create Spark Context (Livy)" node.
 * 
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class LivySparkContextCreatorNodeDialog extends NodeDialogPane implements ChangeListener {

    private final LivySparkContextCreatorNodeSettings m_settings = new LivySparkContextCreatorNodeSettings();
    
    private final DialogComponentStringSelection m_sparkVersion = new DialogComponentStringSelection(
        m_settings.getSparkVersionModel(),
        "Spark version: ",
        new LivySparkContextProvider().getSupportedSparkVersions()
            .stream().map(SparkVersion::getLabel).toArray(String[]::new));
    
    private final DialogComponentString m_livyUrl = new DialogComponentString(m_settings.getLivyUrlModel(), "Livy URL:", true, 35);
    
    private final DialogComponentAuthentication m_authentication = new DialogComponentAuthentication(
        m_settings.getAuthenticationModel(), "Authentication", AuthenticationType.NONE, AuthenticationType.KERBEROS);
    
    private final ContainerResourceDialogPanel m_executorResourcePanel =
        new ContainerResourceDialogPanel("Spark executor", m_settings.getExecutorResources());
    
    private final DialogComponentButtonGroup m_executorAllocation = new DialogComponentButtonGroup(m_settings.getExecutorAllocationModel(), null, false,
        Arrays.stream(ExecutorAllocation.values()).map(ExecutorAllocation::getText).toArray(String[]::new),
        Arrays.stream(ExecutorAllocation.values()).map(ExecutorAllocation::getActionCommand).toArray(String[]::new));
    
    private final JPanel m_executorAllocationOptionsPanel = new JPanel(new CardLayout());
    
    private final DialogComponentNumber m_fixedExecutors =
            new DialogComponentNumber(m_settings.getFixedExecutorsModel(), "Number of executors:", 1, 4);
    
    private final DialogComponentNumber m_dynamicExecutorsMin =
            new DialogComponentNumber(m_settings.getDynamicExecutorsMinModel(), "Minimum number of executors:", 1, 4);

    private final DialogComponentNumber m_dynamicExecutorsMax =
            new DialogComponentNumber(m_settings.getDynamicExecutorsMaxModel(), "Maximum number of executors:", 1, 4);
    
    private final JLabel m_executorResourceSummary = new JLabel();
    
    private final ContainerResourceDialogPanel m_overrideDriverResourcePanel =
        new ContainerResourceDialogPanel("Spark driver", m_settings.getDriverResources());
    
    private final DialogComponentBoolean m_useCustomSparkSettings = new DialogComponentBoolean(m_settings.getUseCustomSparkSettingsModel(),
            "Set custom Spark settings");
    
    private final DialogComponentKeyValueEdit m_customSparkSettings = new DialogComponentKeyValueEdit(m_settings.getCustomSparkSettingsModel());
    
    private final DialogComponentNumber m_connectTimeout = new DialogComponentNumber(m_settings.getConnectTimeoutModel(), 
        "Livy connection timeout (seconds): ", 10, 3);
    
    private final DialogComponentNumber m_responseTimeout = new DialogComponentNumber(m_settings.getResponseTimeoutModel(),
        "Livy response timeout (seconds): ", 10, 3);
    
    private final DialogComponentNumber m_jobCheckFrequency = new DialogComponentNumber(m_settings.getJobCheckFrequencyModel(),
        "Job status polling interval (seconds): ", 1, 3);
    
    private final List<DialogComponent> m_dialogComponents = new ArrayList<DialogComponent>();
    
    /**
     * Constructor.
     */
    LivySparkContextCreatorNodeDialog() {
        addTab("General", createGeneralTab());
        addTab("Advanced", createAdvancedTab());
    }

    private JPanel createAdvancedTab() {

        JPanel panel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.anchor = GridBagConstraints.NORTHWEST;
        gbc.fill = GridBagConstraints.NONE;
        gbc.weightx = 0;
        gbc.weighty = 0;

        gbc.gridx = 0;
        gbc.gridy = 0;
        panel.add(m_overrideDriverResourcePanel, gbc);
        m_overrideDriverResourcePanel.addChangeListener(this);
        
        gbc.gridx = 0;
        gbc.gridy++;
        gbc.insets = new Insets(5, 0, 5, 5);
        addDialogComponentToPanel(m_useCustomSparkSettings, panel, gbc);
        m_useCustomSparkSettings.getModel().addChangeListener(this);

        gbc.gridx = 0;
        gbc.gridy++;
        gbc.weightx = 1;
        gbc.weighty = 1;
        gbc.fill = GridBagConstraints.BOTH;
        gbc.insets = new Insets(5, 15, 5, 15);
        addDialogComponentToPanel(m_customSparkSettings, panel, gbc);
        m_customSparkSettings.getModel().addChangeListener(this);
        gbc.fill = GridBagConstraints.NONE;

        gbc.gridx = 0;
        gbc.gridy++;
        gbc.weightx = 0;
        gbc.weighty = 0;
        gbc.fill = GridBagConstraints.NONE;
        addDialogComponentToPanel(m_connectTimeout, panel, gbc);
        
        gbc.gridx = 0;
        gbc.gridy++;
        addDialogComponentToPanel(m_responseTimeout, panel, gbc);

        gbc.gridx = 0;
        gbc.gridy++;
        addDialogComponentToPanel(m_jobCheckFrequency, panel, gbc);
        
        return panel;
    }

    private JPanel createGeneralTab() {
        JPanel panel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(5, 5, 5, 5);
        gbc.anchor = GridBagConstraints.NORTHWEST;
        gbc.weightx = 0;
        gbc.weighty = 0;

        gbc.gridx = 0;
        gbc.gridy = 0;
        addDialogComponentToPanel(m_sparkVersion, panel, gbc);
        
        gbc.gridx = 0;
        gbc.gridy++;
        addDialogComponentToPanel(m_livyUrl, panel, gbc);
        
        gbc.gridx = 0;
        gbc.gridy++;
        addDialogComponentToPanel(m_authentication, panel, gbc);
        
        gbc.gridx = 0;
        gbc.gridy++;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        panel.add(createSparkExecutorResourcePanel(), gbc);
        
        gbc.gridx = 0;
        gbc.gridy++;
        panel.add(m_executorResourceSummary, gbc);
        m_executorResourceSummary.setFont(m_executorResourceSummary.getFont().deriveFont(Font.PLAIN));
        
        return panel;
    }

    private JPanel createSparkExecutorResourcePanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.anchor = GridBagConstraints.WEST;
        gbc.insets = new Insets(5, 5, 5, 5);
        gbc.gridwidth = 1;
        gbc.gridheight = 1;
        panel.setBorder(BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), " Spark executor resources "));
        
        gbc.gridx = 0;
        gbc.gridy = 0;
        panel.add(m_executorResourcePanel, gbc);
        m_executorResourcePanel.addChangeListener(this);

        gbc.gridx = 0;
        gbc.gridy++;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        panel.add(new JSeparator(SwingConstants.HORIZONTAL), gbc);
        gbc.fill = GridBagConstraints.NONE;
                
        gbc.gridx = 0;
        gbc.gridy++;
        addDialogComponentToPanel(m_executorAllocation, panel, gbc);
        m_executorAllocation.getModel().addChangeListener(this);
        
        m_executorAllocationOptionsPanel.add(new JPanel(), ExecutorAllocation.DEFAULT.getActionCommand());
        m_executorAllocationOptionsPanel.add(createAllocationOptionsPanel(m_fixedExecutors),
            ExecutorAllocation.FIXED.getActionCommand());
        m_fixedExecutors.getModel().addChangeListener(this);
        m_executorAllocationOptionsPanel.add(createAllocationOptionsPanel(m_dynamicExecutorsMin,
            m_dynamicExecutorsMax), ExecutorAllocation.DYNAMIC.getActionCommand());
        m_dynamicExecutorsMin.getModel().addChangeListener(this);
        m_dynamicExecutorsMax.getModel().addChangeListener(this);
        
        gbc.gridx = 0;
        gbc.gridy++;
        gbc.insets = new Insets(0, 20, 5, 5);
        panel.add(m_executorAllocationOptionsPanel, gbc);
        
        return panel;
    }
    
    private JPanel createAllocationOptionsPanel(final DialogComponent ...components) {
        final JPanel container = new JPanel(new FlowLayout(FlowLayout.LEFT, 5, 0));
        final JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.fill = GridBagConstraints.VERTICAL;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.insets = new Insets(0, 0, 0, 0);
        gbc.gridx = 0;
        gbc.gridy = 0;

        for(DialogComponent comp : components) {
            addDialogComponentToPanel(comp, panel, gbc);
            gbc.gridy++;
        }

        container.add(panel);
        return container;
    }
    
    private void addDialogComponentToPanel(DialogComponent comp, JPanel panel, GridBagConstraints gbc) {
        m_dialogComponents.add(comp);
        panel.add(comp.getComponentPanel(), gbc);
    }
    
    private void updateExecutorAllocationOptions() {
        final CardLayout cardLayout = (CardLayout) m_executorAllocationOptionsPanel.getLayout();
        cardLayout.show(m_executorAllocationOptionsPanel, m_settings.getExecutorAllocation().getActionCommand());
    }
    
    private void updateExecutorResourceSummary() {
        final SparkResourceEstimator estimator = new SparkResourceEstimator(m_settings);
        m_executorResourceSummary.setText(estimator.createResourceSummary());
    }

    @Override
    public void stateChanged(final ChangeEvent e) {
        m_settings.updateEnabledness();
        updateExecutorAllocationOptions();
        updateExecutorResourceSummary();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_settings.validateDeeper();
        m_settings.saveSettingsTo(settings);
        
        // We need to do this to trigger validation in some of the dialog components and produce an error
        // when OK or Apply is clicked while invalid values have been entered.
        // Settings models such as SettingsModelString don't accept invalid values. Hence if someone
        // enters an invalid value in the dialog component it will not be put into the settings model.
        // Hence settings model and dialog component go out of sync. If we just save the settings model,
        // it will just save the previous valid value and the dialog will close  with out error
        for (DialogComponent dialogComponent : m_dialogComponents) {
            dialogComponent.saveSettingsTo(settings);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs) throws NotConfigurableException {
        try {
            m_settings.loadSettingsFrom(settings);
            updateExecutorAllocationOptions();
            updateExecutorResourceSummary();
        } catch (InvalidSettingsException e) {
            throw new NotConfigurableException(e.getMessage());
        }
    }
}