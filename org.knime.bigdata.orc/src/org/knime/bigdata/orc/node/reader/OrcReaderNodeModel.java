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
 */

package org.knime.bigdata.orc.node.reader;

import java.io.File;
import java.io.IOException;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObject;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.bigdata.orc.utility.OrcUtility;
import org.knime.cloud.core.file.CloudRemoteFile;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.RowIterator;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.streamable.PartitionInfo;
import org.knime.core.node.streamable.PortInput;
import org.knime.core.node.streamable.PortOutput;
import org.knime.core.node.streamable.RowOutput;
import org.knime.core.node.streamable.StreamableOperator;
import org.knime.core.util.FileUtil;
import org.knime.orc.OrcKNIMEReader;

/**
 * This is the model implementation of OrcReader.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class OrcReaderNodeModel extends NodeModel {

    private final OrcReaderNodeSettings m_settings = new OrcReaderNodeSettings();

    private final OrcUtility m_utility = new OrcUtility();

    /**
     * Constructor for the node model.
     */
    protected OrcReaderNodeModel() {
        super(new PortType[] { ConnectionInformationPortObject.TYPE_OPTIONAL },
                new PortType[] { BufferedDataTable.TYPE });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected BufferedDataTable[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        final ConnectionInformationPortObject connInfoObj = (ConnectionInformationPortObject) inObjects[0];

        final OrcTable table = createORCTable(connInfoObj, exec);
        final BufferedDataTable out = exec.createBufferedDataTable(table, exec);

        return new BufferedDataTable[] { out };
    }

    private OrcTable createORCTable(final ConnectionInformationPortObject connInfoObj, final ExecutionContext exec)
            throws Exception {
        ConnectionInformation connInfo = null;
        if (connInfoObj != null) {
            connInfo = connInfoObj.getConnectionInformation();
        }
        final RemoteFile<Connection> sourceFile;
        if (connInfo instanceof CloudConnectionInformation && connInfo.getProtocol().equalsIgnoreCase("abs")) {
            sourceFile = downloadFilestoLocalFile(connInfo, exec);
        } else {
            sourceFile = m_utility.createRemoteFile(m_settings.getFileName(), connInfo);
        }
        return new OrcTable(sourceFile, m_settings.getReadRowKey(), m_settings.getBatchSize(), exec);
    }

    /**
     * @param connInfo
     * @return
     * @throws Exception
     */
    private RemoteFile<Connection> downloadFilestoLocalFile(ConnectionInformation connInfo, final ExecutionContext exec)
            throws Exception {
        final File tempDir = FileUtil.createTempDir("OrcCloudDownload");
        final CloudRemoteFile<Connection> remoteFile = (CloudRemoteFile<Connection>) m_utility
                .createRemoteFile(m_settings.getFileName(), connInfo);
        RemoteFile<Connection> localFile;
        if (remoteFile.isDirectory()) {
            localFile = m_utility.createLocalFile(tempDir.getPath());
            final CloudRemoteFile<Connection>[] files = remoteFile.listFiles();
            for (final CloudRemoteFile<Connection> file : files) {
                final RemoteFile<Connection> tempFile = m_utility.createLocalFile(localFile.getPath() + file.getName());
                tempFile.write(file, exec);
            }
        } else {
            localFile = m_utility.createLocalFile(tempDir.getPath() + "/" + remoteFile.getName());
            localFile.write(remoteFile, exec);
        }
        return localFile;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reset() {
        // Nothing to do
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        ConnectionInformation connInfo = null;
        final ConnectionInformationPortObjectSpec connectionSpec = (ConnectionInformationPortObjectSpec) inSpecs[0];
        if (connectionSpec != null) {
            connInfo = connectionSpec.getConnectionInformation();
            if (connInfo instanceof CloudConnectionInformation) {
                return new DataTableSpec[] { null };
            }
        }
        try {
            if (connInfo == null) {
                throw new InvalidSettingsException("No connection Information avaiable");
            }
            // Create a reader to get the generated TableSpec
            final RemoteFile<Connection> remoteFile = m_utility.createRemoteFile(m_settings.getFileName(), connInfo);
            if (remoteFile.isDirectory() && remoteFile.listFiles().length == 0) {
                throw new InvalidSettingsException(String.format("Empty directory %s.", m_settings.getFileName()));
            }
            final OrcKNIMEReader reader = new OrcKNIMEReader(remoteFile, m_settings.getReadRowKey(),
                    m_settings.getBatchSize(), null);

            return new DataTableSpec[] { reader.getTableSpec() };

        } catch (final Exception e) {
            throw new InvalidSettingsException(e);
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadSettingsFrom(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadInternals(final File internDir, final ExecutionMonitor exec)
            throws IOException, CanceledExecutionException {
        // Nothing to do
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveInternals(final File internDir, final ExecutionMonitor exec)
            throws IOException, CanceledExecutionException {
        // Nothing to do
    }

    @Override
    public StreamableOperator createStreamableOperator(final PartitionInfo partitionInfo,
            final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        return new StreamableOperator() {

            @Override
            public void runFinal(PortInput[] inputs, PortOutput[] outputs, ExecutionContext exec) throws Exception {
                final PortObject portObj = (PortObject) inputs[0];
                final RowOutput out = (RowOutput) outputs[0];
                final ConnectionInformationPortObject connInfoObj = (ConnectionInformationPortObject) portObj;
                try {
                    final OrcTable table = createORCTable(connInfoObj, exec);
                    final RowIterator rowIterator = table.iterator();
                    while (rowIterator.hasNext()) {
                        out.push(rowIterator.next());
                        exec.checkCanceled();
                    }
                } finally {
                    out.close();
                }
            }
        };
    }

}
