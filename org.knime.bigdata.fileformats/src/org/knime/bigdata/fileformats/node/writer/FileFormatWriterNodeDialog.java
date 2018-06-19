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
import javax.swing.JPanel;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.base.filehandling.remote.dialog.RemoteFileChooser;
import org.knime.base.filehandling.remote.dialog.RemoteFileChooserPanel;
import org.knime.base.filehandling.remote.files.Protocol;
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

/**
 * Node Dialog for generic BigData file format writer.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class FileFormatWriterNodeDialog extends NodeDialogPane {

    private static final String CHUNK_UPLOAD = "Chunk Upload";

    private static final NodeLogger LOGGER = NodeLogger.getLogger(FileFormatWriterNodeDialog.class);
    private final FileFormatWriterNodeSettings m_settings;
    /** textfield to enter file name. */
    private final RemoteFileChooserPanel m_filePanel;

    /**
     * New pane for configuring the generic BigData file format Writer node.
     */
    protected FileFormatWriterNodeDialog(final FileFormatWriterNodeSettings settings) {
        m_settings = settings;
        m_filePanel = new RemoteFileChooserPanel(this.getPanel(), "", false, "targetHistory",
                RemoteFileChooser.SELECT_FILE_OR_DIR,
                createFlowVariableModel(FileFormatWriterNodeSettings.CFGKEY_FILE, FlowVariable.Type.STRING),
                createHDFSConnection());
        final DialogComponentNumberEdit chunkSize = new DialogComponentNumberEdit(m_settings.getChunkSizeModel(),
                "Chunk size in " + m_settings.getChunksizeUnit() + ":");
        final DialogComponentNumberEdit numOfLocalChunks = new DialogComponentNumberEdit(
                m_settings.getNumOfLocalChunksModel(), "Number of local chunks:");

        final JPanel filePanel = new JPanel();
        filePanel.setLayout(new BoxLayout(filePanel, BoxLayout.X_AXIS));
        filePanel.setBorder(BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), "Output location:"));
        filePanel.add(m_filePanel.getPanel());
        filePanel.add(Box.createHorizontalGlue());

        final JPanel gridPanel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.weightx = 1;
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.anchor = GridBagConstraints.LINE_START;
        gbc.fill = GridBagConstraints.HORIZONTAL;

        gridPanel.add(filePanel, gbc);

        ++gbc.gridy;
        gridPanel.add(
                new DialogComponentBoolean(m_settings.getfileOverwritePolicyModel(), "Overwrite").getComponentPanel(),
                gbc);

        ++gbc.gridy;
        gridPanel.add(new DialogComponentStringSelection(m_settings.getCompressionModel(), "File Compression: ",
                m_settings.getCompressionList()).getComponentPanel(), gbc);

        addTab("Options", gridPanel);

        final JPanel chunkUploadPanel = new JPanel(new GridBagLayout());

        chunkUploadPanel.add(chunkSize.getComponentPanel(), gbc);
        ++gbc.gridy;
        chunkUploadPanel.add(numOfLocalChunks.getComponentPanel(), gbc);
        addTab(CHUNK_UPLOAD, chunkUploadPanel);
        setEnabled(false, CHUNK_UPLOAD);

    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_settings.setFileName(m_filePanel.getSelection().trim());
        m_settings.saveSettingsTo(settings);
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
            LOGGER.info("Invalid Settings.", e);
            throw new NotConfigurableException(e.getMessage());
        }

        if (specs.length > 0 && specs[0] != null) {
            final ConnectionInformation connInfo = ((ConnectionInformationPortObjectSpec) specs[0])
                    .getConnectionInformation();
            m_filePanel.setConnectionInformation(connInfo);

            // Enable advanced options for remote connections
            setEnabled(!(connInfo instanceof HDFSLocalConnectionInformation), CHUNK_UPLOAD);

        } else {
            setEnabled(false, CHUNK_UPLOAD);
            // No connection set, create local HDFS Connection
            m_filePanel.setConnectionInformation(createHDFSConnection());
        }
        m_filePanel.setSelection(m_settings.getFileName());
    }

    /**
     * Create the ConnectionInformation for local HDFS.
     *
     * @return ConnectionInformation for a local HDFS connection
     * @throws InvalidSettingsException
     */
    private static ConnectionInformation createHDFSConnection() {
        final HDFSLocalConnectionInformation connectionInformation = new HDFSLocalConnectionInformation();
        final Protocol protocol = HDFSLocalRemoteFileHandler.HDFS_LOCAL_PROTOCOL;
        connectionInformation.setProtocol(protocol.getName());
        connectionInformation.setHost("localhost");
        connectionInformation.setPort(protocol.getPort());
        connectionInformation.setUser(null);
        connectionInformation.setPassword(null);
        return connectionInformation;
    }
}
