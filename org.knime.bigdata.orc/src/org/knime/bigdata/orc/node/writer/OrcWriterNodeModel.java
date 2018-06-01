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
package org.knime.bigdata.orc.node.writer;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObject;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.bigdata.filehandling.local.HDFSLocalConnectionInformation;
import org.knime.bigdata.orc.utility.OrcUtility;
import org.knime.bigdata.parquet.utility.FileUploader;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.streamable.DataTableRowInput;
import org.knime.core.node.streamable.InputPortRole;
import org.knime.core.node.streamable.PartitionInfo;
import org.knime.core.node.streamable.PortInput;
import org.knime.core.node.streamable.PortObjectInput;
import org.knime.core.node.streamable.PortOutput;
import org.knime.core.node.streamable.RowInput;
import org.knime.core.node.streamable.StreamableOperator;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.util.FileUtil;
import org.knime.orc.OrcKNIMEWriter;
import org.knime.orc.OrcTableStoreFormat;
import org.knime.orc.OrcWriteException;

/**
 * This is the model implementation of OrcWriter.
 *
 * @author "Mareike Hoeger, KNIME GmbH, Konstanz, Germany"
 */
public class OrcWriterNodeModel extends NodeModel {
    // the logger instance
    private static final NodeLogger LOGGER = NodeLogger.getLogger(OrcWriterNodeModel.class);

    private static final int PROGRESS_UPDATE_ROW_COUNT = 100;

    private int m_rowCountWritten = 1;

    private final OrcWriterNodeSettings m_settings = new OrcWriterNodeSettings();

    private final OrcUtility m_utility = new OrcUtility();

    /**
     * Constructor for the node model.
     */
    protected OrcWriterNodeModel() {

        super(new PortType[] { ConnectionInformationPortObject.TYPE_OPTIONAL, BufferedDataTable.TYPE }, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected BufferedDataTable[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        final BufferedDataTable input = (BufferedDataTable) inObjects[1];
        if (input.size() == 0) {
            // empty table nothing to do
            LOGGER.debug("Input empty, no file is written");
            return new BufferedDataTable[] {};
        }
        ConnectionInformation connInfo = null;
        final ConnectionInformationPortObject connInfoObj = (ConnectionInformationPortObject) inObjects[0];
        if (connInfoObj != null) {
            connInfo = connInfoObj.getConnectionInformation();
        }
        final RowInput rowInput = new DataTableRowInput(input);
        writeRowInput(exec, rowInput, connInfo);

        return new BufferedDataTable[] {};

    }

    private void writeRowInput(ExecutionContext exec, RowInput input, ConnectionInformation connInfo) throws Exception {
        if (connInfo != null && !(connInfo instanceof HDFSLocalConnectionInformation)) {
            final RemoteFile<Connection> remoteDir = m_utility.createRemoteDir(connInfo, m_settings.getFileName(),
                    m_settings.getFileOverwritePolicy());

            // For remote connections, write data to temporary file and upload
            // the file afterwards.
            writeTempFilesAndUpload(exec, input, remoteDir);
        } else {
            try {
                final RemoteFile<Connection> remoteFile = m_utility.createRemoteFile(m_settings.getFileName(),
                        connInfo);
                m_utility.checkOverwrite(remoteFile, m_settings.getFileOverwritePolicy());
                writeToFile(exec, input, remoteFile, false);
            } catch (final Exception e) {
                throw new OrcWriteException(e);
            }
        }
    }

    private void writeTempFilesAndUpload(final ExecutionContext exec, final RowInput input,
            final RemoteFile<Connection> remoteFile) throws Exception {
        final File path = FileUtil.createTempDir("parquet-cloudupload");
        final ArrayBlockingQueue<RemoteFile<Connection>> tempFileQueue = new ArrayBlockingQueue<>(
                m_settings.getNumOfLocalChunks());
        boolean createNewFile = true;
        int filecount = 0;

        final FileUploader fileUploader = new FileUploader(tempFileQueue, remoteFile,
                exec.createSubExecutionContext(0.5));
        final Thread uploadThread = new Thread(fileUploader, "FileUploader");
        uploadThread.start();
        while (createNewFile) {
            final RemoteFile<Connection> tempFile = m_utility.createTempFile(path, filecount);

            // Write to temporary file.
            createNewFile = writeToFile(exec, input, tempFile, true);
            filecount++;
            LOGGER.info(String.format("Written temporary file %s.", tempFile.getFullName()));
            exec.setMessage(String.format("Written temporary file %s.", tempFile.getFullName()));
            tempFileQueue.put(tempFile);
        }

        fileUploader.setWriteFinished(true);
        uploadThread.join();
    }

    private boolean writeToFile(ExecutionContext exec, final RowInput input, RemoteFile<Connection> tempFile,
            boolean writeChunks) throws InterruptedException, CanceledExecutionException {
        try {
            final OrcKNIMEWriter writer = new OrcKNIMEWriter(tempFile, input.getDataTableSpec(),
                    m_settings.getwriteRowKey(), m_settings.getStripSize(), m_settings.getBatchSize(),
                    m_settings.getCompression());

            for (DataRow row; (row = input.poll()) != null;) {
                if ((m_rowCountWritten % PROGRESS_UPDATE_ROW_COUNT) == 0) {
                    exec.setProgress(String.format("Written row %d.", m_rowCountWritten));
                }

                exec.setMessage("Writing row " + row.getKey());
                writer.writeRow(row);
                m_rowCountWritten++;
                if (writeChunks) {
                    final long size = new File(tempFile.getURI()).length();
                    if (size != 0) {

                        // File is written, stop writing here and create new
                        // file
                        writer.close();
                        return true;
                    }
                }
                exec.checkCanceled();
            }
            writer.close();
            input.close();
            return false;
        } catch (final IOException e) {
            throw new OrcWriteException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reset() {
        // Nothing to do.
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamableOperator createStreamableOperator(final PartitionInfo partitionInfo,
            final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        return new StreamableOperator() {

            @Override
            public void runFinal(PortInput[] inputs, PortOutput[] outputs, ExecutionContext exec) throws Exception {
                ConnectionInformationPortObject connPortObject = null;
                final PortObjectInput portObject = (PortObjectInput) inputs[0];
                if (portObject != null) {
                    connPortObject = (ConnectionInformationPortObject) (portObject).getPortObject();
                }
                ConnectionInformation connInfo = null;
                if (connPortObject != null) {
                    connInfo = connPortObject.getConnectionInformation();
                }
                final RowInput input = (RowInput) inputs[1];
                writeRowInput(exec, input, connInfo);
            }

        };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InputPortRole[] getInputPortRoles() {
        return new InputPortRole[] { InputPortRole.NONDISTRIBUTED_NONSTREAMABLE,
                InputPortRole.NONDISTRIBUTED_STREAMABLE };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected DataTableSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        final ConnectionInformationPortObjectSpec connSpec = (ConnectionInformationPortObjectSpec) inSpecs[0];
        final DataTableSpec tableSpec = (DataTableSpec) inSpecs[1];
        if (connSpec != null) {
            final ConnectionInformation connInfo = connSpec.getConnectionInformation();

            // Check if the port object has connection information
            if (connInfo == null) {
                throw new InvalidSettingsException("No connection information available.");
            }
        } else {

            // Check file access
            CheckUtils.checkDestinationFile(m_settings.getFileName(), m_settings.getFileOverwritePolicy());
        }
        final OrcTableStoreFormat tableformat = new OrcTableStoreFormat();
        if (!tableformat.accepts(tableSpec)) {

            throw new InvalidSettingsException(String.format("Not all types are supported %s",
                    Arrays.toString(tableformat.getUnsupportedTypes(tableSpec))));
        }
        return new DataTableSpec[] {};
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

}
