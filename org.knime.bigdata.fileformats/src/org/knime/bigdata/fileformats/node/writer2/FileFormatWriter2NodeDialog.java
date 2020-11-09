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
 *
 * History
 *   Nov 3, 2020 (dietzc): created
 */
package org.knime.bigdata.fileformats.node.writer2;

import java.awt.Component;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JPanel;

import org.knime.bigdata.fileformats.utility.FileFormatFactory;
import org.knime.core.node.FlowVariableModel;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.context.ports.PortsConfiguration;
import org.knime.core.node.defaultnodesettings.DialogComponentNumberEdit;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;
import org.knime.core.node.defaultnodesettings.SettingsModelNumber;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.datatype.mapping.DataTypeMappingDirection;
import org.knime.datatype.mapping.DataTypeMappingService;
import org.knime.filehandling.core.data.location.variable.FSLocationVariableType;
import org.knime.filehandling.core.defaultnodesettings.filechooser.writer.DialogComponentWriterFileChooser;
import org.knime.filehandling.core.defaultnodesettings.filechooser.writer.SettingsModelWriterFileChooser;
import org.knime.filehandling.core.defaultnodesettings.filtermode.SettingsModelFilterMode.FilterMode;
import org.knime.node.datatype.mapping.DialogComponentDataTypeMapping;

/*
 * @author Christian Dietz, KNIME GmbH, Konstanz Germany
 */
final class FileFormatWriter2NodeDialog<T> extends NodeDialogPane {

    private static final String FILE_HISTORY_ID = "file_format_writer";

    private final FileFormatWriter2Config<T> m_writerConfig;

    private final DialogComponentWriterFileChooser m_filePanel;

    private final DialogComponentDataTypeMapping<T> m_inputTypeMappingComponent;

    private final SettingsModelWriterFileChooser m_fileChooserModel;

    private final SettingsModelNumber m_fileSizeModel;

    private DialogComponentNumberEdit m_fileSizeComponent;

    FileFormatWriter2NodeDialog(final PortsConfiguration portsConfig, final FileFormatFactory<T> factory) {
        m_writerConfig = new FileFormatWriter2Config<>(portsConfig, factory);
        m_fileChooserModel = m_writerConfig.getFileChooserModel();
        m_fileSizeModel = m_writerConfig.getFileSizeModel();
        m_fileSizeComponent = new DialogComponentNumberEdit(m_fileSizeModel,
            "Split data into files of size (" + m_writerConfig.getChunkSizeUnit() + "): ", 6);
        m_fileChooserModel.addChangeListener(e -> updateFileSizeComponent());
        updateFileSizeComponent();

        final FlowVariableModel fvm =
            createFlowVariableModel(m_writerConfig.getLocationKeyChain(), FSLocationVariableType.INSTANCE);
        m_filePanel = new DialogComponentWriterFileChooser(m_fileChooserModel, FILE_HISTORY_ID, fvm, FilterMode.FILE,
            FilterMode.FOLDER);
        m_inputTypeMappingComponent = new DialogComponentDataTypeMapping<>(m_writerConfig.getMappingModel(), true);

        addTab("Settings", createSettingsPanel());
        addTab("Type Mapping", createTypeMappingPanel());
    }

    private void updateFileSizeComponent() {
        if (FilterMode.FOLDER == m_fileChooserModel.getFilterMode()) {
            m_fileSizeModel.setEnabled(true);
            m_fileSizeComponent.setToolTipText(null);
        } else {
            m_fileSizeModel.setEnabled(false);
            m_fileSizeComponent.setToolTipText("Only available when writing to a folder");
        }
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_writerConfig.saveSettingsTo(settings);
    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {

        // general settings & filtering
        try {
            m_writerConfig.loadSettingsFrom(settings);
        } catch (InvalidSettingsException ex) {
            throw new NotConfigurableException("Error during node configuration.", ex);
        }

        m_filePanel.loadSettingsFrom(settings, specs);

        // type mapping settings
        final DataTypeMappingService<T, ?, ?> mappingService = m_writerConfig.getTypeMappingService();
        m_inputTypeMappingComponent.setMappingService(mappingService);
        m_inputTypeMappingComponent.setInputDataTypeMappingConfiguration(
            mappingService.createMappingConfiguration(DataTypeMappingDirection.KNIME_TO_EXTERNAL));
    }

    private Component createTypeMappingPanel() {
        final Box typeMappingBox = new Box(BoxLayout.Y_AXIS);
        typeMappingBox.add(Box.createHorizontalGlue());
        typeMappingBox.add(m_inputTypeMappingComponent.getComponentPanel());
        typeMappingBox.add(Box.createHorizontalGlue());
        return typeMappingBox;
    }

    private Component createSettingsPanel() {
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.weightx = 1;
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.anchor = GridBagConstraints.FIRST_LINE_START;
        gbc.insets = new Insets(5, 0, 5, 0);
        gbc.fill = GridBagConstraints.HORIZONTAL;

        final JPanel mainOptionsPanel = new JPanel(new GridBagLayout());

        final JPanel filePanel = m_filePanel.getComponentPanel();
        filePanel.setBorder(BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), "Output location"));
        mainOptionsPanel.add(filePanel, gbc);

        gbc.gridy++;
        gbc.fill = GridBagConstraints.NONE;
        mainOptionsPanel.add(new DialogComponentStringSelection(m_writerConfig.getCompressionModel(),
            "File Compression: ", m_writerConfig.getCompressionList()).getComponentPanel(), gbc);

        gbc.gridy++;
        mainOptionsPanel.add(m_fileSizeComponent.getComponentPanel(), gbc);

        gbc.gridy++;
        mainOptionsPanel.add(new DialogComponentNumberEdit(m_writerConfig.getChunkSizeModel(),
            "Within file " + m_writerConfig.getChunkUnit().toLowerCase()
            + " size (" + m_writerConfig.getChunkSizeUnit() + "): ", 6).getComponentPanel(), gbc);

        return mainOptionsPanel;
    }
}
