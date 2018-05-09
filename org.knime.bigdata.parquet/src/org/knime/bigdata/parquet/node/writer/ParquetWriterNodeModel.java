/*
 * ------------------------------------------------------------------------
 * Copyright by KNIME AG, Zurich, Switzerland Website: http://www.knime.com;
 * Email: contact@knime.com This program is free software; you can redistribute
 * it and/or modify it under the terms of the GNU General Public License,
 * Version 3, as published by the Free Software Foundation. This program is
 * distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU General Public License for more details. You
 * should have received a copy of the GNU General Public License along with this
 * program; if not, see <http://www.gnu.org/licenses>. Additional permission
 * under GNU GPL version 3 section 7: KNIME interoperates with ECLIPSE solely
 * via ECLIPSE's plug-in APIs. Hence, KNIME and ECLIPSE are both independent
 * programs and are not derived from each other. Should, however, the
 * interpretation of the GNU GPL Version 3 ("License") under any applicable laws
 * result in KNIME and ECLIPSE being a combined program, KNIME AG herewith
 * grants you the additional permission to use and propagate KNIME together with
 * ECLIPSE with only the license terms in place for ECLIPSE applying to ECLIPSE
 * and the GNU GPL Version 3 applying for KNIME, provided the license terms of
 * ECLIPSE themselves allow for the respective use and propagation of ECLIPSE
 * together with KNIME. Additional permission relating to nodes for KNIME that
 * extend the Node Extension (and in particular that are based on subclasses of
 * NodeModel, NodeDialog, and NodeView) and that only interoperate with KNIME
 * through standard APIs ("Nodes"): Nodes are deemed to be separate and
 * independent programs and to not be covered works. Notwithstanding anything to
 * the contrary in the License, the License does not apply to Nodes, you are not
 * required to license Nodes under the License, and you are granted a license to
 * prepare and propagate Nodes, in each case even if such Nodes are propagated
 * with or for interoperation with KNIME. The owner of a Node may freely choose
 * the license terms applicable to such Node, including when such Node is
 * propagated with or for interoperation with KNIME.
 * ------------------------------------------------------------------- History
 * 13.03.2018 (Mareike Hoeger): created
 */
package org.knime.bigdata.parquet.node.writer;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObject;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.bigdata.filehandling.local.HDFSLocalConnectionInformation;
import org.knime.bigdata.parquet.utility.FileUploader;
import org.knime.bigdata.parquet.utility.ParquetTableStoreWriter;
import org.knime.bigdata.parquet.utility.ParquetUtility;
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

/**
 * This is the model implementation of ParquetWriter.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class ParquetWriterNodeModel extends NodeModel {

    private static final int PROGRESS_UPDATE_ROW_COUNT = 100;

    // the logger instance
    private static final NodeLogger LOGGER = NodeLogger.getLogger(ParquetWriterNodeModel.class);

    private final ParquetWriterNodeSettings m_settings = new ParquetWriterNodeSettings();

    private int m_rowCountWritten = 1;

    private ArrayBlockingQueue<RemoteFile<Connection>> m_tempFileQueue;

    private final ParquetUtility m_utility = new ParquetUtility();

    /**
     * Constructor for the node model.
     */
    protected ParquetWriterNodeModel() {
        super(new PortType[] { ConnectionInformationPortObject.TYPE_OPTIONAL, BufferedDataTable.TYPE }, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected BufferedDataTable[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        final BufferedDataTable input = (BufferedDataTable) inObjects[1];

        if (input.size() == 0) {

            // empty table nothing to do.
            LOGGER.debug("Input empty, no file is written.");
            return new BufferedDataTable[] {};
        }
        final ConnectionInformationPortObject connInfo = (ConnectionInformationPortObject) inObjects[0];
        final RowInput rowInput = new DataTableRowInput(input);
        writeRowInput(exec, rowInput, connInfo);

        return new BufferedDataTable[] {};
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

                final RowInput input = (RowInput) inputs[1];
                writeRowInput(exec, input, connPortObject);
            }
        };
    }

    private void writeRowInput(final ExecutionContext exec, final RowInput input,
            final ConnectionInformationPortObject connInfo) throws Exception {

        if (connInfo != null && !(connInfo.getConnectionInformation() instanceof HDFSLocalConnectionInformation)) {
            final RemoteFile<Connection> remoteDir = m_utility.createRemoteDir(connInfo, m_settings.getFileName(),
                    m_settings.getFileOverwritePolicy());

            // For remote connections, write data to temporary file and upload
            // the file afterwards.
            writeTempFilesAndUpload(exec, input, remoteDir);
        } else {
            final RemoteFile<Connection> remoteFile = m_utility.createLocalFile(m_settings.getFileName(),
                    m_settings.getFileOverwritePolicy());

            // For local or HDFS_local connection write directly.
            writeToFile(exec, input, remoteFile, false);
        }
    }

    private void writeTempFilesAndUpload(final ExecutionContext exec, final RowInput input,
            final RemoteFile<Connection> remoteFile) throws Exception {
        final File path = FileUtil.createTempDir("parquet-cloudupload");
        m_tempFileQueue = new ArrayBlockingQueue<>(m_settings.getNumOfLocalChunks());
        boolean createNewFile = true;
        int filecount = 0;

        final FileUploader fileUploader = new FileUploader(m_tempFileQueue, remoteFile,
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
            m_tempFileQueue.put(tempFile);
        }

        fileUploader.setWriteFinished(true);
        uploadThread.join();
    }

    private boolean writeToFile(final ExecutionContext exec, final RowInput input,
            final RemoteFile<Connection> remoteFile, boolean writeChunks) throws Exception {
        final DataTableSpec dataSpec = input.getDataTableSpec();
        final boolean writeRowKey = m_settings.getwriteRowKey();
        final Path filePath = new Path(remoteFile.getURI());
        final CompressionCodecName codec = CompressionCodecName.fromConf(m_settings.getCompression());
        final int chunkSize = m_settings.getChunkSize() * 1024 * 1024; // Bytes

        exec.setMessage("Starting to write File.");
        try (final ParquetTableStoreWriter writer = new ParquetTableStoreWriter(dataSpec, writeRowKey, filePath, codec,
                chunkSize)) {

            for (DataRow row; (row = input.poll()) != null;) {

                if ((m_rowCountWritten % PROGRESS_UPDATE_ROW_COUNT) == 0) {
                    exec.setProgress(String.format("Written row %d.", m_rowCountWritten));
                }
                writer.writeRow(row);
                m_rowCountWritten++;

                if (writeChunks) {
                    final long size = new File(remoteFile.getURI()).length();
                    if (size != 0) {

                        // File is written, stop writing here and create new
                        // file
                        return true;
                    }
                }
                exec.checkCanceled();
            }
            return false;

        } catch (final CanceledExecutionException cee) {
            try {
                remoteFile.delete();
                LOGGER.debug(
                        String.format("File '%s' deleted after node has been canceled.", m_settings.getFileName()));
            } catch (final IOException ex) {
                LOGGER.warn(String.format("Unable to delete file '%s' after cancellation: %s.",
                        m_settings.getFileName(), ex.getMessage()), ex);
            }
            throw cee;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reset() {
        m_rowCountWritten = 1;
        if (m_tempFileQueue != null) {
            m_tempFileQueue.clear();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected DataTableSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        final ConnectionInformationPortObjectSpec connSpec = (ConnectionInformationPortObjectSpec) inSpecs[0];
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
        // nothing to do
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveInternals(final File internDir, final ExecutionMonitor exec)
            throws IOException, CanceledExecutionException {
        // nothing to do
    }

    @Override
    public InputPortRole[] getInputPortRoles() {
        return new InputPortRole[] { InputPortRole.NONDISTRIBUTED_NONSTREAMABLE,
                InputPortRole.NONDISTRIBUTED_STREAMABLE };
    }

}
