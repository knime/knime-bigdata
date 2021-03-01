/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 */
package org.knime.bigdata.spark.local.node.create;

import static org.knime.bigdata.spark.local.node.create.LocalSparkContextSettings.WorkingDirMode.MANUAL;
import static org.knime.bigdata.spark.local.node.create.LocalSparkContextSettings.WorkingDirMode.USER_HOME;
import static org.knime.bigdata.spark.local.node.create.LocalSparkContextSettings.WorkingDirMode.WORKFLOW_DATA_AREA;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.border.Border;
import javax.swing.border.EtchedBorder;
import javax.swing.border.TitledBorder;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.bigdata.spark.local.node.create.LocalSparkContextSettings.WorkingDirMode;
import org.knime.bigdata.spark.node.util.context.create.time.TimeDialogPanel;
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
import org.knime.core.node.util.FileSystemBrowser.DialogType;
import org.knime.core.node.util.FileSystemBrowser.FileSelectionMode;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.connections.local.LocalFSConnection;
import org.knime.filehandling.core.connections.local.LocalFileSystem;
import org.knime.filehandling.core.defaultnodesettings.fileselection.FileSelectionDialog;

/**
 * Node dialog for the "Create Local Big Data Environment" node with working directory chooser and advanced tab.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class LocalEnvironmentCreatorNodeDialog3 extends NodeDialogPane implements ChangeListener {

    private static final String WORKING_DIR_HISTORY_ID = "local.workingDir";

    private static final String HIVE_DIR_HISTORY_ID = "local.workingDir";

    private final LocalSparkContextSettings m_settings;

    private final TimeDialogPanel m_timeShift;

    private final boolean m_hasWorkingDirectorySetting;

    private final DialogComponentButtonGroup m_workingDirMode;

    private final ChangeListener m_workingDirListener = e -> updateManualWorkingDirSetting();

    private final FileSelectionDialog m_manualWorkingDirChooser =
        new FileSelectionDialog(WORKING_DIR_HISTORY_ID, //
            25, // history length
            this::createFSConnection, //
            DialogType.SAVE_DIALOG, //
            FileSelectionMode.DIRECTORIES_ONLY, //
            new String[0], //
            e -> {});

    private final ChangeListener m_hiveDirListener = e -> updateHiveDirSetting();

    private final FileSelectionDialog m_hiveDirChooser =
            new FileSelectionDialog(HIVE_DIR_HISTORY_ID, //
                25, // history length
                this::createFSConnection, //
                DialogType.SAVE_DIALOG, //
                FileSelectionMode.DIRECTORIES_ONLY, //
                new String[0], //
                e -> {});

    /**
     * Constructor.
     */
    LocalEnvironmentCreatorNodeDialog3(final boolean hasWorkingDirectorySetting) {
        m_hasWorkingDirectorySetting = hasWorkingDirectorySetting;
        m_settings = new LocalSparkContextSettings(hasWorkingDirectorySetting);

        m_workingDirMode = new DialogComponentButtonGroup(m_settings.getWorkingDirectoryModeModel(), null, false,
            new WorkingDirMode[]{MANUAL, USER_HOME, WORKFLOW_DATA_AREA});

        addTab("Settings", createSettingsTab());
        m_timeShift = new TimeDialogPanel(m_settings.getTimeShiftSettings());
        addTab("Time", m_timeShift);
        addTab("Advanced", createAdvancedTab());
    }

    private JPanel createSparkContextPanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        panel.setBorder(createTitledBorder("Spark context"));
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.anchor = GridBagConstraints.WEST;

        gbc.gridx = 0;
        gbc.gridy++;
        gbc.gridwidth = 1;
        gbc.insets = new Insets(0, 5, 5, 5);
        panel.add(new JLabel("Context name:"), gbc);
        gbc.gridx++;
        gbc.insets = new Insets(0, 0, 5, 5);
        panel.add(new DialogComponentString(m_settings.getContextNameModel(), null, true, 30).getComponentPanel(),
            gbc);

        gbc.gridx = 0;
        gbc.gridy++;
        gbc.gridwidth = 1;
        gbc.insets = new Insets(0, 5, 5, 5);
        panel.add(new JLabel("Number of threads:"), gbc);
        gbc.gridx++;
        gbc.insets = new Insets(0, 0, 5, 5);
        panel.add(new DialogComponentNumber(m_settings.getNumberOfThreadsModel(), null, 1, 5).getComponentPanel(), gbc);

        gbc.gridx = 0;
        gbc.gridy++;
        gbc.gridwidth = 1;
        gbc.insets = new Insets(8, 5, 5, 5);
        gbc.anchor = GridBagConstraints.NORTHWEST;
        panel.add(new JLabel("On dispose:"), gbc);
        gbc.gridx++;
        gbc.insets = new Insets(0, 5, 5, 5);
        panel.add(new DialogComponentButtonGroup(m_settings.getOnDisposeActionModel(),
            null, true, LocalSparkContextSettings.OnDisposeAction.values()).getComponentPanel(), gbc);

        gbc.gridx = 0;
        gbc.gridy++;
        gbc.gridwidth = 1;
        gbc.insets = new Insets(8, 5, 5, 5);
        panel.add(new JLabel("SQL support:"), gbc);
        gbc.gridx++;
        gbc.insets = new Insets(0, 5, 5, 5);
        panel.add(new DialogComponentButtonGroup(m_settings.getSqlSupportModel(),
            null, true, LocalSparkContextSettings.SQLSupport.values()).getComponentPanel(), gbc);
        m_settings.getSqlSupportModel().addChangeListener(this);

        // space on right side to move everything to the left
        gbc.gridx++;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.weightx = 1;
        panel.add(new JLabel(), gbc);

        return panel;
    }

    private JComponent createFileSystemPanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        panel.setBorder(createTitledBorder("File System settings"));
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.gridx = gbc.gridy = 0;
        gbc.insets = new Insets(0, 5, 0, 5);
        gbc.anchor = GridBagConstraints.WEST;
        panel.add(new JLabel("Working directory:"), gbc);
        gbc.gridx++;
        gbc.weightx = 1;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.insets = new Insets(0, 0, 0, 0);
        panel.add(m_workingDirMode.getButton(MANUAL.getActionCommand()), gbc);
        gbc.gridy++;
        gbc.insets = new Insets(0, 20, 0, 0);
        panel.add(m_manualWorkingDirChooser.getPanel(), gbc);
        gbc.gridy++;
        gbc.insets = new Insets(0, 0, 0, 0);
        panel.add(m_workingDirMode.getButton(USER_HOME.getActionCommand()), gbc);
        gbc.gridy++;
        gbc.insets = new Insets(0, 0, 0, 0);
        panel.add(m_workingDirMode.getButton(WORKFLOW_DATA_AREA.getActionCommand()), gbc);

        m_workingDirMode.getModel().addChangeListener(this);

        return panel;
    }

    private JPanel createSettingsTab() {
        final JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.gridx = gbc.gridy = 0;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.weightx = 1;
        panel.add(createSparkContextPanel(), gbc);
        gbc.gridy++;
        panel.add(createFileSystemPanel(), gbc);
        gbc.gridy++;
        gbc.fill = GridBagConstraints.BOTH;
        gbc.weighty = 1;
        panel.add(new JLabel(), gbc);
        return panel;
    }

    private JPanel createExistingSparkContextPanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        panel.setBorder(createTitledBorder("Existing Spark context"));
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.gridx = gbc.gridy = 0;
        gbc.insets = new Insets(0, 0, 0, 0);
        gbc.anchor = GridBagConstraints.NORTHWEST;
        gbc.gridwidth = 1;
        panel.add(new DialogComponentBoolean(m_settings.getHideExistsWarningModel(),
                "Hide warning about an existing local Spark context").getComponentPanel(), gbc);
        gbc.gridx++;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.weightx = 1;
        panel.add(new JLabel(), gbc);
        return panel;
    }

    private JPanel createCustomSparkSettingsPanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        panel.setBorder(createTitledBorder("Custom Spark settings"));
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.gridx = gbc.gridy = 0;
        gbc.insets = new Insets(0, 0, 0, 0);
        gbc.anchor = GridBagConstraints.NORTHWEST;
        gbc.gridwidth = 1;
        panel.add(new DialogComponentBoolean(m_settings.getUseCustomSparkSettingsModel(), "Use custom Spark settings")
            .getComponentPanel(), gbc);
        gbc.gridx++;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.weightx = 1;
        panel.add(new JLabel(), gbc);
        gbc.gridx = 0;
        gbc.gridy++;
        gbc.gridwidth = 2;
        gbc.insets = new Insets(5, 5, 5, 5);
        gbc.fill = GridBagConstraints.BOTH;
        gbc.weighty = 1;
        panel.add(new DialogComponentMultiLineString(m_settings.getCustomSparkSettingsModel(),
            null, true, 40, 5).getComponentPanel(), gbc);

        m_settings.getUseCustomSparkSettingsModel().addChangeListener(this);

        return panel;
    }

    private JPanel createCustomHiveSettingsPanel() {
        m_settings.getUseHiveDataFolderModel().addChangeListener(this);

        final JPanel panel = new JPanel(new GridBagLayout());
        panel.setBorder(createTitledBorder("Hive settings"));
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.gridx = gbc.gridy = 0;
        gbc.insets = new Insets(0, 0, 0, 0);
        gbc.anchor = GridBagConstraints.NORTHWEST;
        gbc.gridwidth = 1;
        panel.add(new DialogComponentBoolean(m_settings.getUseHiveDataFolderModel(), "Use custom Hive data folder (Metastore DB & Warehouse)")
            .getComponentPanel());
        gbc.gridx++;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.weightx = 1;
        panel.add(new JLabel(), gbc);
        gbc.gridx = 0;
        gbc.gridy++;
        gbc.gridwidth = 2;
        gbc.insets = new Insets(0, 0, 5, 0);
        panel.add(m_hiveDirChooser.getPanel(), gbc);

        return panel;
    }

    private JPanel createAdvancedTab() {
        final JPanel advancedPanel = new JPanel(new GridBagLayout());
        final GridBagConstraints advancedPanelGbc = new GridBagConstraints();
        advancedPanelGbc.gridx = advancedPanelGbc.gridy = 0;
        advancedPanelGbc.fill = GridBagConstraints.BOTH;
        advancedPanelGbc.weightx = 1;
        advancedPanelGbc.weighty = 1;
        advancedPanel.add(createCustomSparkSettingsPanel(), advancedPanelGbc);
        advancedPanelGbc.gridy++;
        advancedPanelGbc.fill = GridBagConstraints.HORIZONTAL;
        advancedPanelGbc.weighty = 0;
        advancedPanel.add(createExistingSparkContextPanel(), advancedPanelGbc);
        advancedPanelGbc.gridy++;
        advancedPanel.add(createCustomHiveSettingsPanel(), advancedPanelGbc);
        return advancedPanel;
    }

    private static Border createTitledBorder(final String title) {
        return new TitledBorder(new EtchedBorder(EtchedBorder.RAISED), title);
    }

    @Override
    public void stateChanged(final ChangeEvent e) {
    	final Object eventSource = e.getSource();

        if (eventSource == m_settings.getUseCustomSparkSettingsModel() //
            || eventSource == m_settings.getSqlSupportModel() //
            || eventSource == m_settings.getUseHiveDataFolderModel() //
            || eventSource == m_settings.getWorkingDirectoryModeModel()) {

            updateEnabledness();
        }
    }

	private void updateEnabledness() {
        m_settings.updateEnabledness();
        m_hiveDirChooser.setEnabled(m_settings.isHiveEnabled() && m_settings.useHiveDataFolder());
        m_manualWorkingDirChooser.setEnabled(m_settings.getWorkingDirectoryMode() == MANUAL);
	}

    private void updateManualWorkingDirSetting() {
        m_settings.getManualWorkingDirectoryModel().setStringValue(m_manualWorkingDirChooser.getSelected());
    }

    private void updateHiveDirSetting() {
        m_settings.getHiveDataFolderModel().setStringValue(m_hiveDirChooser.getSelected());
    }

    private FSConnection createFSConnection() {
        return new LocalFSConnection(m_settings.getManualWorkingDirectory(), LocalFileSystem.CONNECTED_FS_LOCATION_SPEC);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_settings.validateDeeper();
        m_settings.saveSettingsTo(settings);

        m_manualWorkingDirChooser.addCurrentSelectionToHistory();
        m_hiveDirChooser.addCurrentSelectionToHistory();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs) throws NotConfigurableException {
        try {
            m_settings.loadSettingsFrom(settings);
            m_hiveDirChooser.setSelected(m_settings.getHiveDataFolderModel().getStringValue());
            m_hiveDirChooser.addListener(m_hiveDirListener);
            updateEnabledness();

            if (m_hasWorkingDirectorySetting) {
                m_manualWorkingDirChooser.setSelected(m_settings.getManualWorkingDirectory());
                m_manualWorkingDirChooser.addListener(m_workingDirListener);
            }

        } catch (InvalidSettingsException e) {
            throw new NotConfigurableException(e.getMessage());
        }
    }

    @Override
    public void onClose() {
        m_hiveDirChooser.removeListener(m_hiveDirListener);
        m_hiveDirChooser.onClose();

        if (m_hasWorkingDirectorySetting) {
            m_manualWorkingDirChooser.removeListener(m_workingDirListener);
            m_manualWorkingDirChooser.onClose();
        }
    }
}