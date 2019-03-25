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
package org.knime.bigdata.fileformats.node.writer;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.UIManager;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.base.filehandling.remote.dialog.RemoteFileChooser;
import org.knime.base.filehandling.remote.dialog.RemoteFileChooserPanel;
import org.knime.bigdata.filehandling.local.HDFSLocalConnectionInformation;
import org.knime.bigdata.filehandling.local.HDFSLocalRemoteFileHandler;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentNumberEdit;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.datatype.mapping.DataTypeMappingDirection;
import org.knime.datatype.mapping.DataTypeMappingService;
import org.knime.node.datatype.mapping.DialogComponentDataTypeMapping;

/**
 * Node Dialog for generic BigData file format writer.
 *
 * @param <X> the type whose instances describe the external data types
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class FileFormatWriterNodeDialog<X> extends NodeDialogPane implements ChangeListener {

    private static final String WARNING_MESSAGE =
        "<html>The remote writing creates/overwrites a " + "<u>folder</u> with the given name.</html>";

    private static final String CHUNK_UPLOAD = "Chunk Upload";

    private boolean m_chunkUpload = false;

    private static final NodeLogger LOGGER = NodeLogger.getLogger(FileFormatWriterNodeDialog.class);

    private final FileFormatWriterNodeSettings<X> m_settings;

    /** textfield to enter file name. */
    private final RemoteFileChooserPanel m_filePanel;

    private final DialogComponentDataTypeMapping<X> m_inputTypeMappingComponent;

    private final JLabel m_warningLable;

    /**
     * New pane for configuring the generic BigData file format Writer node.
     *
     * @param settings the settings for the node dialog
     */
    protected FileFormatWriterNodeDialog(final FileFormatWriterNodeSettings<X> settings) {
        m_settings = settings;
        m_filePanel = new RemoteFileChooserPanel(this.getPanel(), "", false, "targetHistory",
            RemoteFileChooser.SELECT_FILE_OR_DIR,
            createFlowVariableModel(FileFormatWriterNodeSettings.CFGKEY_FILE, FlowVariable.Type.STRING),
            HDFSLocalConnectionInformation.getInstance());
        final DialogComponentNumberEdit chunkSize = new DialogComponentNumberEdit(m_settings.getChunkSizeModel(),
            "Chunk size in " + m_settings.getChunksizeUnit() + ":");
        final DialogComponentNumberEdit numOfLocalChunks =
            new DialogComponentNumberEdit(m_settings.getNumOfLocalChunksModel(), "Number of local chunks:");

        final JPanel filePanel = new JPanel();
        filePanel.setLayout(new BoxLayout(filePanel, BoxLayout.X_AXIS));
        filePanel.setBorder(BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), "Output location:"));
        filePanel.add(m_filePanel.getPanel());
        filePanel.add(Box.createHorizontalGlue());

        m_warningLable = new JLabel(WARNING_MESSAGE);
        m_warningLable.setIcon(UIManager.getIcon("OptionPane.warningIcon"));
        m_warningLable.setVisible(false);

        final JPanel gridPanel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.weightx = 1;
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.anchor = GridBagConstraints.LINE_START;
        gbc.fill = GridBagConstraints.HORIZONTAL;

        gridPanel.add(filePanel, gbc);

        ++gbc.gridy;
        gridPanel.add(m_warningLable, gbc);
        ++gbc.gridy;
        gbc.fill = GridBagConstraints.NONE;
        m_settings.getfileOverwritePolicyModel().addChangeListener(this);
        final JPanel overwritePanel =
            new DialogComponentBoolean(m_settings.getfileOverwritePolicyModel(), "Overwrite").getComponentPanel();
        gridPanel.add(overwritePanel, gbc);

        ++gbc.gridy;
        m_settings.getcheckDirContentModel().setEnabled(false);
        m_settings.getcheckDirContentModel().addChangeListener(this);
        final JPanel check_panel =
            new DialogComponentBoolean(m_settings.getcheckDirContentModel(), "Check directory content")
                .getComponentPanel();
        gridPanel.add(check_panel, gbc);

        ++gbc.gridy;
        gridPanel.add(new DialogComponentStringSelection(m_settings.getCompressionModel(), "File Compression: ",
            m_settings.getCompressionList()).getComponentPanel(), gbc);

        addTab("Options", gridPanel);

        final JPanel chunkUploadPanel = new JPanel(new GridBagLayout());

        chunkUploadPanel.add(chunkSize.getComponentPanel(), gbc);
        ++gbc.gridy;
        chunkUploadPanel.add(numOfLocalChunks.getComponentPanel(), gbc);
        addTab(CHUNK_UPLOAD, chunkUploadPanel);
        setEnabled(m_chunkUpload, CHUNK_UPLOAD);

        final Box typeMappingBox = new Box(BoxLayout.Y_AXIS);
        typeMappingBox.add(Box.createHorizontalGlue());
        m_inputTypeMappingComponent = new DialogComponentDataTypeMapping<>(m_settings.getMappingModel(), true);
        typeMappingBox.add(m_inputTypeMappingComponent.getComponentPanel());
        typeMappingBox.add(Box.createHorizontalGlue());
        addTab("Type Mapping", typeMappingBox);

    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {
        m_warningLable.setVisible(false);
        m_settings.getcheckDirContentModel().setEnabled(false);
        try {
            m_settings.loadSettingsFrom(settings);
        } catch (final InvalidSettingsException e) {
            LOGGER.info("Invalid Settings.", e);
            throw new NotConfigurableException(e.getMessage());
        }

        if (specs.length > 0 && specs[0] != null) {
            final ConnectionInformation connInfo =
                ((ConnectionInformationPortObjectSpec)specs[0]).getConnectionInformation();
            m_filePanel.setConnectionInformation(connInfo);

            // Enable advanced options for remote connections
            final boolean isHDFSLocal =
                connInfo.getProtocol().equalsIgnoreCase(HDFSLocalRemoteFileHandler.HDFS_LOCAL_PROTOCOL.getName());
            m_chunkUpload = !isHDFSLocal;
            setEnabled(m_chunkUpload, CHUNK_UPLOAD);

            if (m_settings.getFileOverwritePolicy()) {
                m_warningLable.setVisible(true);
                m_settings.getcheckDirContentModel().setEnabled(true);
            }
        } else {
            m_chunkUpload = false;
            setEnabled(m_chunkUpload, CHUNK_UPLOAD);
            // No connection set, create local HDFS Connection
            m_filePanel.setConnectionInformation(HDFSLocalConnectionInformation.getInstance());
        }
        m_filePanel.setSelection(m_settings.getFileNameWithSuffix());

        final DataTypeMappingService<X, ?, ?> mappingService = m_settings.getFormatFactory().getTypeMappingService();
        m_inputTypeMappingComponent.setMappingService(mappingService);
        m_inputTypeMappingComponent.setInputDataTypeMappingConfiguration(
            mappingService.createMappingConfiguration(DataTypeMappingDirection.KNIME_TO_EXTERNAL));
        m_inputTypeMappingComponent.loadSettingsFrom(settings, specs);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_inputTypeMappingComponent.saveSettingsTo(settings);
        m_settings.setFileName(m_filePanel.getSelection().trim());
        m_settings.saveSettingsTo(settings);
    }

    @Override
    public void stateChanged(final ChangeEvent e) {
        if (m_settings.getfileOverwritePolicyModel().getBooleanValue() && m_chunkUpload) {
            m_warningLable.setVisible(true);
            m_settings.getcheckDirContentModel().setEnabled(true);
        } else {
            m_warningLable.setVisible(false);
            m_settings.getcheckDirContentModel().setEnabled(false);
        }
    }

}
