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
 *   Created on Aug 10, 2016 by sascha
 */
package org.knime.bigdata.spark.node.io.genericdatasource.writer;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;

import javax.swing.DefaultComboBoxModel;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JSpinner;
import javax.swing.SpinnerNumberModel;
import javax.swing.event.AncestorEvent;
import javax.swing.event.AncestorListener;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.apache.commons.lang3.StringUtils;
import org.knime.base.filehandling.NodeUtils;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.base.filehandling.remote.dialog.RemoteFileChooser;
import org.knime.base.filehandling.remote.dialog.RemoteFileChooserPanel;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.FlowVariableModel;
import org.knime.core.node.FlowVariableModelButton;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.util.StringHistory;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterPanel;
import org.knime.core.node.workflow.FlowVariable;

import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.port.SparkContextProvider;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.core.preferences.KNIMEConfigContainer;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.bigdata.spark.node.SparkSaveMode;

/**
 * @author Sascha Wolke, KNIME.com
 * @param <T> Settings type used by this node
 */
public class Spark2GenericDataSourceNodeDialog<T extends Spark2GenericDataSourceSettings> extends NodeDialogPane implements ActionListener {

    /** Internal settings model */
    protected final T m_settings;

    private final FlowVariableModel m_directoryFlowVariable;
    private final RemoteFileChooserPanel m_directoryChooser;

    private final JComboBox<String> m_outputName;
    private final FlowVariableModel m_outputNameFlowVariable;
    private final FlowVariableModelButton m_outputNameFlowVariableButton;

    private final JComboBox<SparkSaveMode> m_saveMode;

    private final JCheckBox m_uploadDriver;

    private final JCheckBox m_overwriteNumPartitions;
    private final JSpinner m_numPartitions;

    private final DataColumnSpecFilterPanel m_partitionColumns;

    /** Main option panel. */
    protected final JPanel m_optionsPanel;

    /** Grid bag constraints used on main options panel. */
    protected final GridBagConstraints m_optionsPanelConstraints;

    /**
     * Default constructor.
     * @see #addToOptionsPanel(String, JComponent) for customization
     * @param initialSettings - Initial settings object
     */
    public Spark2GenericDataSourceNodeDialog(final T initialSettings) {
        m_settings = initialSettings;

        m_optionsPanel = new JPanel(new GridBagLayout());
        m_optionsPanelConstraints = new GridBagConstraints();
        NodeUtils.resetGBC(m_optionsPanelConstraints);

        m_directoryFlowVariable = createFlowVariableModel(Spark2GenericDataSourceSettings.CFG_DIRECTORY, FlowVariable.Type.STRING);
        m_directoryChooser = new RemoteFileChooserPanel(getPanel(), "Target dir", false,
            "outputDirSpark_" + m_settings.getFormat(),
            RemoteFileChooser.SELECT_DIR, m_directoryFlowVariable, null);
        addToOptionsPanel("Target folder" , m_directoryChooser.getPanel());

        m_outputName = new JComboBox<>();
        m_outputName.setEditable(true);
        m_outputNameFlowVariable = createFlowVariableModel(Spark2GenericDataSourceSettings.CFG_NAME, FlowVariable.Type.STRING);
        m_outputNameFlowVariable.addChangeListener( new ChangeListener() {
            @Override
            public void stateChanged(final ChangeEvent e) {
                boolean replacement = m_outputNameFlowVariable.isVariableReplacementEnabled();
                Optional<FlowVariable> variableValue = m_outputNameFlowVariable.getVariableValue();
                m_outputName.setEnabled(!replacement);
                if (replacement && variableValue.isPresent()) {
                    m_outputName.setSelectedItem(variableValue.get().getStringValue());
                }
            }
        });
        m_optionsPanel.addAncestorListener(new AncestorListener() {
            @Override
            public void ancestorRemoved(final AncestorEvent event) {}

            @Override
            public void ancestorMoved(final AncestorEvent event) {}

            @Override
            public void ancestorAdded(final AncestorEvent event) {
                if (m_outputNameFlowVariable.isVariableReplacementEnabled() && m_outputNameFlowVariable.getVariableValue().isPresent()) {
                    String newPath = m_outputNameFlowVariable.getVariableValue().get().getStringValue();
                    String oldPath = getSelection(m_outputName);
                    if (!StringUtils.equals(newPath, oldPath)) {
                        m_outputName.setSelectedItem(newPath);
                    }
                }
            }
        });
        m_outputNameFlowVariableButton = new FlowVariableModelButton(m_outputNameFlowVariable);
        addToOptionsPanel("Target name", outputNamePanel());

        m_saveMode = new JComboBox<>(SparkSaveMode.ALL);
        m_saveMode.setEditable(false);
        addToOptionsPanel("Save mode", m_saveMode);

        if (m_settings.hasDriver()) {
            m_uploadDriver = new JCheckBox("Upload data source driver");
            addToOptionsPanel("Driver", m_uploadDriver);
        } else {
            m_uploadDriver = null;
        }

        m_overwriteNumPartitions = new JCheckBox("Overwrite result partition count:");
        m_overwriteNumPartitions.addActionListener(this);
        m_numPartitions = new JSpinner(new SpinnerNumberModel(1, 1, 10000, 1));
        JPanel overwriteNumPartitionPanel = new JPanel(new GridLayout(1, 2));
        overwriteNumPartitionPanel.add(m_overwriteNumPartitions);
        overwriteNumPartitionPanel.add(m_numPartitions);
        addToOptionsPanel("Partitions", overwriteNumPartitionPanel);

        addTab("Options", m_optionsPanel);

        if (m_settings.supportsPartitioning()) {
            JPanel panel = new JPanel(new GridBagLayout());
            GridBagConstraints gbc = new GridBagConstraints();
            NodeUtils.resetGBC(gbc);
            m_partitionColumns = new DataColumnSpecFilterPanel(true);
            panel.add(m_partitionColumns, gbc);
            addTab("Partitions", panel);
        } else {
            m_partitionColumns = null;
        }
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

    private JPanel outputNamePanel() {
        JPanel panel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        NodeUtils.resetGBC(gbc);
        gbc.insets = new Insets(0, 0, 0, 5);
        gbc.weightx = 1;
        panel.add(m_outputName, gbc);
        gbc.weightx = 0;
        gbc.fill = GridBagConstraints.NONE;
        gbc.gridx++;
        gbc.insets = new Insets(0, 0, 0, 0);
        panel.add(m_outputNameFlowVariableButton, gbc);
        return panel;
    }

    @Override
    public void actionPerformed(final ActionEvent e) {
        if (e.getSource().equals(m_overwriteNumPartitions)) {
            m_numPartitions.setEnabled(m_overwriteNumPartitions.isSelected());

            if (m_overwriteNumPartitions.isSelected()) {
                JOptionPane.showMessageDialog(getPanel(),
                    "Small partition counts with huge data sets results in performance issues. Use with caution!",
                    "Performance warning",
                    JOptionPane.WARNING_MESSAGE);
            }
        }
    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
            throws NotConfigurableException {

        ConnectionInformation connInfo = null;
        DataTableSpec dataTableSpec = null;

        if (specs.length > 0 && specs[0] != null) {
            connInfo = ((ConnectionInformationPortObjectSpec) specs[0]).getConnectionInformation();
        }

        if (specs.length > 1 && specs[1] != null) {
            dataTableSpec = ((SparkDataPortObjectSpec) specs[1]).getTableSpec();
        }

        m_settings.loadConfigurationInDialog(settings, dataTableSpec);
        m_directoryChooser.setConnectionInformation(connInfo);
        m_directoryChooser.setSelection(m_settings.getDirectory());

        updateHistory("outputDirSpark_" + m_settings.getFormat(), m_outputName, new String[0]);
        m_outputName.setSelectedItem(m_settings.getName());
        m_saveMode.setSelectedItem(m_settings.getSparkSaveMode());

        if (m_settings.hasDriver()) {
            m_uploadDriver.setSelected(m_settings.uploadDriver());
        }

        if (m_settings.supportsPartitioning()) {
            if (dataTableSpec != null) {
                m_partitionColumns.loadConfiguration(m_settings.getPartitionBy(), dataTableSpec);
                m_partitionColumns.setEnabled(true);
            } else {
                m_partitionColumns.setEnabled(false);
            }
        }

        m_overwriteNumPartitions.setSelected(m_settings.overwriteNumPartitions());
        m_numPartitions.setValue(m_settings.getNumPartitions());
        m_numPartitions.setEnabled(m_overwriteNumPartitions.isSelected());
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_settings.setDirectory(m_directoryChooser.getSelection());
        m_settings.setName(getSelection(m_outputName));
        StringHistory.getInstance("outputDirSpark_" + m_settings.getFormat(), 15).add(getSelection(m_outputName));
        m_settings.setSaveMode(getSaveModeSelection());

        if (m_settings.hasDriver()) {
            m_settings.setUploadDriver(m_uploadDriver.isSelected());
        }

        if (m_settings.supportsPartitioning() && m_partitionColumns.isEnabled()) {
            m_partitionColumns.saveConfiguration(m_settings.getPartitionBy());
        }

        m_settings.setOverwriteNumPartitions(m_overwriteNumPartitions.isSelected());
        m_settings.setNumPartitions(((SpinnerNumberModel) m_numPartitions.getModel()).getNumber().intValue());

        m_settings.validateSettings();
        m_settings.saveSettingsTo(settings);
    }

    private SparkSaveMode getSaveModeSelection() {
        return (SparkSaveMode) m_saveMode.getSelectedItem();
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
     * @param defaults - Default values to always add
     */
    protected void updateHistory(final String id, final JComboBox<String> comboBox, final String[] defaults) {
        final StringHistory history = StringHistory.getInstance(id, 15);
        final Set<String> set = new LinkedHashSet<>();
        Collections.addAll(set, history.getHistory());
        Collections.addAll(set, defaults);
        final DefaultComboBoxModel<String> model = (DefaultComboBoxModel<String>) comboBox.getModel();
        model.removeAllElements();
        for (final String string : set) {
            model.addElement(string);
        }
    }

    /**
     * Set combo box elements.
     * @param comboBox desired box
     * @param elements all box elements
     */
    protected void setAllElements(final JComboBox<String> comboBox, final String[] elements) {
        final DefaultComboBoxModel<String> model = (DefaultComboBoxModel<String>) comboBox.getModel();
        model.removeAllElements();
        for (final String element : elements) {
            model.addElement(element);
        }
    }

    /**
     * Get spark version from port specs.
     * @param specs spark port spec with version or null
     * @return spark version from spec or default context
     */
    protected SparkVersion getSparkVersion(final PortObjectSpec[] specs) {
        if (specs != null) {
            for (PortObjectSpec spec : specs) {
                if (spec != null && spec instanceof SparkContextProvider) {
                    return SparkContextUtil.getSparkVersion(((SparkContextProvider)spec).getContextID());
                }
            }
        }

        return KNIMEConfigContainer.getSparkVersion();
    }
}
