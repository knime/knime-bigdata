/*
 * ------------------------------------------------------------------------
 * Copyright by KNIME AG, Zurich, Switzerland Website: http://www.knime.com;
 * Email: contact@knime.com
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, Version 3, as published by the
 * Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, see <http://www.gnu.org/licenses>.
 *
 * Additional permission under GNU GPL version 3 section 7:
 *
 * KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs. Hence,
 * KNIME and ECLIPSE are both independent programs and are not derived from each
 * other. Should, however, the interpretation of the GNU GPL Version 3
 * ("License") under any applicable laws result in KNIME and ECLIPSE being a
 * combined program, KNIME AG herewith grants you the additional permission to
 * use and propagate KNIME together with ECLIPSE with only the license terms in
 * place for ECLIPSE applying to ECLIPSE and the GNU GPL Version 3 applying for
 * KNIME, provided the license terms of ECLIPSE themselves allow for the
 * respective use and propagation of ECLIPSE together with KNIME.
 *
 * Additional permission relating to nodes for KNIME that extend the Node
 * Extension (and in particular that are based on subclasses of NodeModel,
 * NodeDialog, and NodeView) and that only interoperate with KNIME through
 * standard APIs ("Nodes"): Nodes are deemed to be separate and independent
 * programs and to not be covered works. Notwithstanding anything to the
 * contrary in the License, the License does not apply to Nodes, you are not
 * required to license Nodes under the License, and you are granted a license to
 * prepare and propagate Nodes, in each case even if such Nodes are propagated
 * with or for interoperation with KNIME. The owner of a Node may freely choose
 * the license terms applicable to such Node, including when such Node is
 * propagated with or for interoperation with KNIME.
 * -------------------------------------------------------------------
 *
 * History 28.05.2018 (Mareike Hoeger): created
 */
package org.knime.bigdata.fileformats.node.reader;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JPanel;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.base.filehandling.remote.dialog.RemoteFileChooser;
import org.knime.base.filehandling.remote.dialog.RemoteFileChooserPanel;
import org.knime.bigdata.filehandling.local.HDFSLocalConnectionInformation;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.datatype.mapping.DataTypeMappingDirection;
import org.knime.datatype.mapping.DataTypeMappingService;
import org.knime.node.datatype.mapping.DialogComponentDataTypeMapping;

/**
 * Node dialog for generic file format reader.
 *
 * @param <X> the type whose instances describe the external data types
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class FileFormatReaderNodeDialog<X> extends NodeDialogPane {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(FileFormatReaderNodeDialog.class);
    private final FileFormatReaderNodeSettings<X> m_settings;
    /** textfield to enter file name. */
    private final RemoteFileChooserPanel m_filePanel;
    private final DialogComponentDataTypeMapping<X> m_outputTypeMappingComponent;

    /**
     * New pane for configuring the OrcWriter node.
     */
    protected FileFormatReaderNodeDialog(final FileFormatReaderNodeSettings<X> settings) {
        m_settings = settings;
        m_filePanel = new RemoteFileChooserPanel(this.getPanel(), "", false, "targetHistory",
                RemoteFileChooser.SELECT_FILE_OR_DIR,
                createFlowVariableModel(FileFormatReaderNodeSettings.CFGKEY_FILE, FlowVariable.Type.STRING),
                HDFSLocalConnectionInformation.getInstance());
        final JPanel filePanel = new JPanel();
        filePanel.setLayout(new BoxLayout(filePanel, BoxLayout.X_AXIS));
        filePanel.setBorder(BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), "Input:"));
        filePanel.add(m_filePanel.getPanel());
        filePanel.add(Box.createHorizontalGlue());

        final JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.weightx = 1;
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.anchor = GridBagConstraints.LINE_START;
        gbc.fill = GridBagConstraints.HORIZONTAL;

        panel.add(filePanel, gbc);

        addTab("Options", panel);

        final Box typeMappingBox = new Box(BoxLayout.Y_AXIS);
        typeMappingBox.add(Box.createHorizontalGlue());
        m_outputTypeMappingComponent = new DialogComponentDataTypeMapping<>(m_settings.getMappingModel(), true);
        typeMappingBox.add(m_outputTypeMappingComponent.getComponentPanel());
        typeMappingBox.add(Box.createHorizontalGlue());

        addTab("Type Mapping", typeMappingBox);

    }

    /**
     * {@inheritDoc}
     */
	@Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
            throws NotConfigurableException {
        try {
            m_settings.loadSettingsFrom(settings);
        } catch (final InvalidSettingsException e) {
            LOGGER.info("Invalid settigns.", e);
            throw new NotConfigurableException(e.getMessage());
        }
        if (specs.length > 0 && specs[0] != null) {
            final ConnectionInformation connInfo = ((ConnectionInformationPortObjectSpec) specs[0])
                    .getConnectionInformation();
            m_filePanel.setConnectionInformation(connInfo);
        } else {
            // No connection set, create local HDFS Connection
            m_filePanel.setConnectionInformation(HDFSLocalConnectionInformation.getInstance());
        }
        final DataTypeMappingService<X, ?, ?> mappingService = m_settings.getFormatFactory().getTypeMappingService();
        m_outputTypeMappingComponent.setInputDataTypeMappingConfiguration(
        		mappingService.createMappingConfiguration(DataTypeMappingDirection.EXTERNAL_TO_KNIME));
        m_filePanel.setSelection(m_settings.getFileName());
        m_outputTypeMappingComponent.setMappingService(mappingService);
        m_outputTypeMappingComponent.loadSettingsFrom(settings, specs);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_settings.setFileName(m_filePanel.getSelection().trim());
        m_outputTypeMappingComponent.saveSettingsTo(settings);
        m_settings.saveSettingsTo(settings);
    }
}
