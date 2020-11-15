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
package org.knime.bigdata.spark.node.io.genericdatasource.filehandling.writer;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JSpinner;
import javax.swing.SpinnerNumberModel;
import javax.swing.border.Border;

import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.port.SparkContextProvider;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.core.preferences.KNIMEConfigContainer;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.FlowVariableModel;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.util.StringHistory;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterPanel;
import org.knime.filehandling.core.data.location.variable.FSLocationVariableType;
import org.knime.filehandling.core.defaultnodesettings.filechooser.writer.DialogComponentWriterFileChooser;
import org.knime.filehandling.core.defaultnodesettings.filtermode.SettingsModelFilterMode.FilterMode;

/**
 * @author Sascha Wolke, KNIME.com
 * @param <T> Settings type used by this node
 */
public class Spark2GenericDataSourceNodeDialog3<T extends Spark2GenericDataSourceSettings3> extends NodeDialogPane implements ActionListener {

    private static final String FILE_HISTORY_ID = "spark_generic_reader_writer";

    /** Internal settings model */
    protected final T m_settings;

    private DialogComponentWriterFileChooser m_outputPathChooser;

    private JCheckBox m_uploadDriver;

    private JCheckBox m_overwriteNumPartitions;
    private JSpinner m_numPartitions;

    private DataColumnSpecFilterPanel m_partitionColumns;

    /**
     * Default constructor.
     *
     * @param initialSettings - Initial settings object
     * @see #addSettingsPanels(JPanel, GridBagConstraints) for customization
     */
    public Spark2GenericDataSourceNodeDialog3(final T initialSettings) {
        m_settings = initialSettings;
        addTab("Settings", createSettingsTab());
        addTab("Partitions", createPartitionsTab());
    }

    private JPanel createSettingsTab() {
        final JPanel panel = new JPanel();
        panel.setLayout(new GridBagLayout());
        final GridBagConstraints settingsGbc = new GridBagConstraints();
        settingsGbc.gridx = settingsGbc.gridy = 0;
        settingsGbc.fill = GridBagConstraints.HORIZONTAL;
        settingsGbc.weightx = 1;

        final JPanel outputPathPanel = new JPanel();
        outputPathPanel.setLayout(new BoxLayout(outputPathPanel, BoxLayout.LINE_AXIS));
        outputPathPanel.setBorder(createTitledBorder("Output location"));
        FlowVariableModel fvm = createFlowVariableModel(m_settings.getFileChooserModel().getKeysForFSLocation(),
            FSLocationVariableType.INSTANCE);
        m_outputPathChooser = new DialogComponentWriterFileChooser(m_settings.getFileChooserModel(), FILE_HISTORY_ID,
            fvm, FilterMode.FOLDER);
        outputPathPanel.add(Box.createHorizontalStrut(5));
        outputPathPanel.add(m_outputPathChooser.getComponentPanel());
        outputPathPanel.add(Box.createHorizontalStrut(5));
        panel.add(outputPathPanel, settingsGbc);
        settingsGbc.gridy++;

        if (m_settings.hasDriver()) {
            final JPanel driverPanel = new JPanel();
            driverPanel.setLayout(new BoxLayout(driverPanel, BoxLayout.LINE_AXIS));
            driverPanel.setBorder(createTitledBorder("Driver"));
            m_uploadDriver = new JCheckBox("Upload data source driver");
            driverPanel.add(Box.createHorizontalStrut(5));
            driverPanel.add(m_uploadDriver);
            driverPanel.add(Box.createHorizontalGlue());
            panel.add(driverPanel, settingsGbc);
            settingsGbc.gridy++;
        }

        addSettingsPanels(panel, settingsGbc); // NOSONAR safe to be overwritten
        addVerticalSpace(panel, settingsGbc);

        return panel;
    }

    private JPanel createPartitionsTab() {
        final JPanel partPanel = new JPanel();
        partPanel.setLayout(new GridBagLayout());
        final GridBagConstraints partGbc = new GridBagConstraints();
        partGbc.gridx = partGbc.gridy = 0;

        if (m_settings.supportsPartitioning()) {
            partGbc.weighty = 1;
            partGbc.fill = GridBagConstraints.HORIZONTAL | GridBagConstraints.VERTICAL;
            m_partitionColumns = new DataColumnSpecFilterPanel(true);
            partPanel.add(m_partitionColumns, partGbc);
            partGbc.gridy++;
        }

        JPanel overwriteNumPartitionPanel = new JPanel();
        overwriteNumPartitionPanel.setLayout(new BoxLayout(overwriteNumPartitionPanel, BoxLayout.LINE_AXIS));
        overwriteNumPartitionPanel.add(Box.createHorizontalGlue());
        m_overwriteNumPartitions = new JCheckBox("Overwrite partition count:");
        m_overwriteNumPartitions.addActionListener(this);
        overwriteNumPartitionPanel.add(m_overwriteNumPartitions);
        m_numPartitions = new JSpinner(new SpinnerNumberModel(1, 1, 10000, 1));
        overwriteNumPartitionPanel.add(m_numPartitions);
        overwriteNumPartitionPanel.add(Box.createHorizontalGlue());
        partGbc.weighty = 0;
        partGbc.fill = GridBagConstraints.HORIZONTAL;
        partGbc.insets = new Insets(10, 10, 10, 10);
        partPanel.add(overwriteNumPartitionPanel, partGbc);

        // add some space at the end
        if (!m_settings.supportsPartitioning()) {
            addVerticalSpace(partPanel, partGbc);
        }

        return partPanel;
    }

    /**
     * @param title title of the border
     * @return border with title
     */
    protected Border createTitledBorder(final String title) {
        return BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), title);
    }

    /**
     * Add additional panels to the settings panel.
     *
     * @param settingsPanel panel to add additional panels
     * @param gbc grid bag constraint of the settings panel
     */
    protected void addSettingsPanels(final JPanel settingsPanel, final GridBagConstraints gbc) {
        // overwrite this if required
    }

    /**
     * Add components with description to a given panel.
     *
     * @param panel panel to add components to
     * @param gbc grid bag constraint of the given panel
     * @param description text on the left
     * @param component component on the right
     */
    protected void addToPanel(final JPanel panel, final GridBagConstraints gbc, final String description, final JComponent component) {
        gbc.gridx = 0;
        gbc.gridy++;
        gbc.weightx = 0;
        panel.add(new JLabel(description + ": "), gbc);
        gbc.gridx++;
        gbc.weightx = 1;
        panel.add(component, gbc);
    }

    /**
     * Add second component to a panel (after first component with description added).
     *
     * @param panel panel to add components to
     * @param gbc grid bag constraint of the given panel
     * @param component - Component on the right
     */
    protected void addToPanel(final JPanel panel, final GridBagConstraints gbc, final JComponent component) {
        gbc.gridy++;
        gbc.weightx = 1;
        panel.add(component, gbc);
    }

    private static final void addVerticalSpace(final JPanel panel, final GridBagConstraints gbc) {
        gbc.gridy++;
        gbc.weighty = 1;
        gbc.fill = GridBagConstraints.HORIZONTAL | GridBagConstraints.VERTICAL;
        panel.add(new JLabel(), gbc);
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

        final DataTableSpec dataTableSpec;
        if (specs.length == 1 && specs[0] != null) {
            dataTableSpec = ((SparkDataPortObjectSpec) specs[0]).getTableSpec();
        } else if (specs.length == 2 && specs[1] != null) {
            dataTableSpec = ((SparkDataPortObjectSpec) specs[1]).getTableSpec();
        } else {
            dataTableSpec = null;
        }

        m_settings.loadSettingsDialog(settings, dataTableSpec);
        m_outputPathChooser.loadSettingsFrom(settings, specs);

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
        m_outputPathChooser.saveSettingsTo(settings);

        if (m_settings.hasDriver()) {
            m_settings.setUploadDriver(m_uploadDriver.isSelected());
        }

        if (m_settings.supportsPartitioning() && m_partitionColumns.isEnabled()) {
            m_partitionColumns.saveConfiguration(m_settings.getPartitionBy());
        }

        m_settings.setOverwriteNumPartitions(m_overwriteNumPartitions.isSelected());
        m_settings.setNumPartitions(((SpinnerNumberModel) m_numPartitions.getModel()).getNumber().intValue());

        m_settings.validateSettings();
        m_settings.saveSettingsToDialog(settings);
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
                if (spec instanceof SparkContextProvider) {
                    return SparkContextUtil.getSparkVersion(((SparkContextProvider)spec).getContextID());
                }
            }
        }

        return KNIMEConfigContainer.getSparkVersion();
    }
}
