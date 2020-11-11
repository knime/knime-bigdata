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
 *   Created on Aug 9, 2016 by sascha
 */
package org.knime.bigdata.spark.node.io.genericdatasource.filehandling.reader;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.util.function.Consumer;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JCheckBox;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.border.Border;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.node.SparkSourceNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPreviewPanel;
import org.knime.bigdata.spark.core.port.data.SparkDataTable;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.FlowVariableModel;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.filehandling.core.data.location.variable.FSLocationVariableType;
import org.knime.filehandling.core.defaultnodesettings.filechooser.reader.DialogComponentReaderFileChooser;
import org.knime.filehandling.core.defaultnodesettings.filtermode.SettingsModelFilterMode.FilterMode;
import org.knime.filehandling.core.defaultnodesettings.status.StatusMessage;
import org.knime.filehandling.core.defaultnodesettings.status.StatusMessage.MessageType;

/**
 * Generic Spark reader dialog.
 *
 * @author Sascha Wolke, KNIME GmbH
 * @param <T> Settings type.
 */
public class GenericDataSource2SparkNodeDialog3<T extends GenericDataSource2SparkSettings3> extends NodeDialogPane {

    private static final String FILE_HISTORY_ID = "spark_generic_reader_writer";

    /** Internal settings model */
    protected final T m_settings;

    private final DialogComponentReaderFileChooser m_inputPathChooser;

    private final JCheckBox m_uploadDriver;

    private final Consumer<StatusMessage> m_messageConsumer = createMessageConsumer();

    private final ChangeListener m_inputPathChangeListener = createInputPathChangeListener();

    private final SparkDataPreviewPanel m_previewPanel;

    private SparkContextID m_contextId;

    /**
     * Default constructor.
     * @see #addSettingsPanels(JPanel, GridBagConstraints)
     * @param initialSettings - Initial settings object
     */
    public GenericDataSource2SparkNodeDialog3(final T initialSettings) {
        m_settings = initialSettings;

        final JPanel settingsPanel = new JPanel();
        settingsPanel.setLayout(new GridBagLayout());
        final GridBagConstraints settingsGbc = new GridBagConstraints();
        settingsGbc.gridx = settingsGbc.gridy = 0;
        settingsGbc.fill = GridBagConstraints.HORIZONTAL;
        settingsGbc.weightx = 1;

        final JPanel inputPathPanel = new JPanel();
        inputPathPanel.setBorder(createTitledBorder("Input location"));
        inputPathPanel.setLayout(new BoxLayout(inputPathPanel, BoxLayout.LINE_AXIS));
        final FlowVariableModel fvm = createFlowVariableModel(m_settings.getFileChooserModel().getKeysForFSLocation(),
            FSLocationVariableType.INSTANCE);
        m_inputPathChooser = new DialogComponentReaderFileChooser(m_settings.getFileChooserModel(), FILE_HISTORY_ID,
            fvm, FilterMode.FILE, FilterMode.FOLDER);
        inputPathPanel.add(Box.createHorizontalStrut(5));
        inputPathPanel.add(m_inputPathChooser.getComponentPanel());
        inputPathPanel.add(Box.createHorizontalStrut(5));
        settingsPanel.add(inputPathPanel, settingsGbc);
        settingsGbc.gridy++;

        if (m_settings.hasDriver()) {
            final JPanel driverPanel = new JPanel();
            driverPanel.setBorder(BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), "Driver"));
            m_uploadDriver = new JCheckBox("Upload data source driver");
            driverPanel.add(m_uploadDriver);
            settingsPanel.add(driverPanel, settingsGbc);
            settingsGbc.gridy++;
        } else {
            m_uploadDriver = null;
        }

        addSettingsPanels(settingsPanel, settingsGbc); // NOSONAR safe to be overwritten
        addVerticalSpace(settingsPanel, settingsGbc);
        addTab("Settings", settingsPanel);

        m_previewPanel = new SparkDataPreviewPanel() {
            private static final long serialVersionUID = -8340686067208065014L;

            @Override
            protected SparkDataTable prepareDataTable(final ExecutionMonitor exec) throws Exception {
                return GenericDataSource2SparkNodeModel3.preparePreview(m_settings, m_contextId, exec, m_messageConsumer);
            }
        };
        addTab("Preview", m_previewPanel, false);
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

    private static final void addVerticalSpace(final JPanel panel, final GridBagConstraints gbc) {
        gbc.gridy++;
        gbc.weighty = 1;
        gbc.fill = GridBagConstraints.HORIZONTAL | GridBagConstraints.VERTICAL;
        panel.add(new JLabel(), gbc);
    }

    /**
     * @param title title of the border
     * @return border with title
     */
    protected Border createTitledBorder(final String title) {
        return BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), title);
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

    private ChangeListener createInputPathChangeListener() {
        return new ChangeListener() {
            @Override
            public void stateChanged(final ChangeEvent e) {
                m_previewPanel.reset();
            }
        };
    }

    private Consumer<StatusMessage> createMessageConsumer() {
        return new Consumer<StatusMessage>() {
            @Override
            public void accept(final StatusMessage t) {
                if (t.getType() == MessageType.ERROR) {
                    getLogger().error(t.getMessage());
                } else if (t.getType() == MessageType.WARNING) {
                    getLogger().warn(t.getMessage());
                } else {
                    getLogger().info(t.getMessage());
                }
            }
        };
    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {

        m_settings.loadSettingsFromDialog(settings);
        m_inputPathChooser.loadSettingsFrom(settings, specs);

        if (m_settings.hasDriver()) {
            m_uploadDriver.setSelected(m_settings.uploadDriver());
        }

        m_contextId = SparkSourceNodeModel.getContextID(specs);
        m_previewPanel.reset();

        if (!m_inputPathChooser.getSettingsModel().canCreateConnection()) {
            m_previewPanel.setDisabled("No file system connection available. Execute connector node.");
        }
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_inputPathChooser.saveSettingsTo(settings);

        if (m_settings.hasDriver()) {
            m_settings.setUploadDriver(m_uploadDriver.isSelected());
        }

        m_settings.validateSettings();
        m_settings.saveSettingsToDialog(settings);
    }

    @Override
    public void onOpen() {
        super.onOpen();
        m_settings.getFileChooserModel().addChangeListener(m_inputPathChangeListener);
    }

    @Override
    public void onClose() {
        super.onClose();
        m_settings.getFileChooserModel().removeChangeListener(m_inputPathChangeListener);
    }
}
