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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.eclipse.core.runtime.URIUtil;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObject;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.base.filehandling.remote.files.RemoteFileFactory;
import org.knime.bigdata.fileformats.utility.BigDataFileFormatException;
import org.knime.bigdata.fileformats.utility.FileHandlingUtility;
import org.knime.bigdata.fileformats.utility.FileUploader;
import org.knime.bigdata.filehandling.local.HDFSLocalRemoteFileHandler;
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
import org.knime.datatype.mapping.DataTypeMappingConfiguration;
import org.knime.datatype.mapping.DataTypeMappingService;

/**
 * Generic node model for BigData file format writer.
 *
 * @param <X> the type whose instances describe the external data types
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class FileFormatWriterNodeModel<X> extends NodeModel {

    private static final int PROGRESS_UPDATE_ROW_COUNT = 100;

    // the logger instance
    private static final NodeLogger LOGGER = NodeLogger.getLogger(FileFormatWriterNodeModel.class);

    private final FileFormatWriterNodeSettings<X> m_settings;

    private int m_rowCountWritten = 1;

    /**
     * Constructor for the node model.
     */
    protected FileFormatWriterNodeModel(final FileFormatWriterNodeSettings<X> settings) {
        super(new PortType[] { ConnectionInformationPortObject.TYPE_OPTIONAL, BufferedDataTable.TYPE }, null);
        m_settings = settings;

    }

    /**
     * Checks if the dir contains files, that do not end with the right suffix and
     * gives a warning if it does.
     *
     * @param connInfo
     * @param fileName
     * @throws Exception
     */
    private void checkDirContent(final ConnectionInformation connInfo, final String fileName) throws Exception {

            final ConnectionMonitor<Connection> conMonitor = new ConnectionMonitor<>();
            final URI fileURI = connInfo.toURI().resolve(URIUtil.fromString(fileName));
            final RemoteFile<Connection> remotefile = RemoteFileFactory.createRemoteFile(fileURI, connInfo, conMonitor);
            if (remotefile.isDirectory()) {
                final RemoteFile<Connection>[] fileList = remotefile.listFiles();
                for (final RemoteFile<Connection> file : fileList) {
                    final String name = file.getName();
                    // ignore known Spark and HDFS metafiles
                    if (!name.equalsIgnoreCase("_SUCCESS") && !name.endsWith("crc")) {
                        final String suffix = m_settings.getFilenameSuffix();
                        if (!name.endsWith(suffix)) {
                            throw new BigDataFileFormatException(String.format(
                                    "The directory contains files without '%s' suffix. "
                                            + "The directory will be lost with the current settings.",
                                            m_settings.getFilenameSuffix()));
                        }
                    }
                }
            }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected DataTableSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {

    	initializeTypeMappingIfNecessary();
        final ConnectionInformationPortObjectSpec connSpec = (ConnectionInformationPortObjectSpec) inSpecs[0];
        if (connSpec != null && !connSpec.getConnectionInformation().getProtocol()
                .equalsIgnoreCase(HDFSLocalRemoteFileHandler.HDFS_LOCAL_PROTOCOL.getName())) {

            final ConnectionInformation connInfo = connSpec.getConnectionInformation();

            // Check if the port object has connection information
            if (connInfo == null) {
                throw new InvalidSettingsException("No connection information available.");
            }
            final String fileName = m_settings.getFileName();

            if (fileName.endsWith("/")) {
                m_settings.setFileName(fileName.substring(0, fileName.length() - 1));
            }
            try {
                final URI uri = new URI(m_settings.getFileName());
                if (uri.getScheme() != null) {
                    connInfo.fitsToURI(uri);
                }
            } catch (final Exception e) {
                LOGGER.debug("Could not configure node.", e);
                throw new InvalidSettingsException(e.getMessage());
            }

        } else {

            // Check file access
            CheckUtils.checkDestinationFile(m_settings.getFileNameWithSuffix(), m_settings.getFileOverwritePolicy());
        }
        return new DataTableSpec[] {};
    }

    /**
     * This method ensures that the default type mapping of the give {@link DataTypeMappingService} is copied into the
     * node models {@link DataTypeMappingConfiguration}s if they are empty which is the case when a new node is created.
     * The node dialog itself prevents the user from removing all type mappings so they can only be empty during
     * Initialisation.
     */
    private void initializeTypeMappingIfNecessary() {
        try {
        	final DataTypeMappingService<X, ?, ?> mappingService = m_settings.getFormatFactory().getTypeMappingService();
            final DataTypeMappingConfiguration<X> origExternalToKnimeConf =
            		m_settings.getMappingModel().getDataTypeMappingConfiguration(mappingService);
            if (origExternalToKnimeConf.getTypeRules().isEmpty() && origExternalToKnimeConf.getNameRules().isEmpty()) {
                final DataTypeMappingConfiguration<X> combinedConf =
                        origExternalToKnimeConf.with(mappingService.newDefaultKnimeToExternalMappingConfiguration());
                m_settings.getMappingModel().setDataTypeMappingConfiguration(combinedConf);
            }
        } catch (final InvalidSettingsException e) {
            LOGGER.warn("Could not initialize type mapping with default rules.", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamableOperator createStreamableOperator(final PartitionInfo partitionInfo,
            final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        return new StreamableOperator() {

            @Override
            public void runFinal(final PortInput[] inputs, final PortOutput[] outputs, final ExecutionContext exec)
                    throws Exception {
                ConnectionInformationPortObject connPortObject = null;
                final PortObjectInput portObject = (PortObjectInput) inputs[0];
                if (portObject != null) {
                    connPortObject = (ConnectionInformationPortObject) portObject.getPortObject();
                }

                final RowInput input = (RowInput) inputs[1];
                writeRowInput(exec, input, connPortObject);
            }
        };
    }

	private AbstractFileFormatWriter createWriter(final RowInput input, final RemoteFile<Connection> remoteFile,
    		final DataTypeMappingConfiguration<X> mappingConfiguration)
    		throws IOException {
        final DataTableSpec dataSpec = input.getDataTableSpec();
        final int chunkSize = m_settings.getChunkSize();
        final String compression = m_settings.getCompression();
        return m_settings.getFormatFactory().getWriter(remoteFile, dataSpec, chunkSize, compression,
        		mappingConfiguration);

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
        if(m_settings.getcheckDirContent()) {
            checkDirContent(connInfo.getConnectionInformation(), m_settings.getFileName());
        }
        final RowInput rowInput = new DataTableRowInput(input);
        writeRowInput(exec, rowInput, connInfo);

        return new BufferedDataTable[] {};
    }

    @Override
    public InputPortRole[] getInputPortRoles() {
        return new InputPortRole[] { InputPortRole.NONDISTRIBUTED_NONSTREAMABLE,
                InputPortRole.NONDISTRIBUTED_STREAMABLE };
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
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        initializeTypeMappingIfNecessary();
        m_settings.loadSettingsFrom(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reset() {
        m_rowCountWritten = 1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveInternals(final File internDir, final ExecutionMonitor exec)
            throws IOException, CanceledExecutionException {
        // nothing to do
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
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.validateSettings(settings);
    }

    private void writeRowInput(final ExecutionContext exec, final RowInput input,
            final ConnectionInformationPortObject connInfo) throws Exception {

        if (connInfo != null && !connInfo.getConnectionInformation().getProtocol()
                .equalsIgnoreCase(HDFSLocalRemoteFileHandler.HDFS_LOCAL_PROTOCOL.getName())) {
            final RemoteFile<Connection> remoteDir = FileHandlingUtility.createRemoteDir(connInfo,
                    m_settings.getFileName());
            FileHandlingUtility.checkOverwrite(remoteDir, m_settings.getFileOverwritePolicy());
            // For remote connections, write data to temporary file and upload
            // the file afterwards.
            writeTempFilesAndUpload(exec, input, remoteDir);
        } else {
            final RemoteFile<Connection> remoteFile = FileHandlingUtility
                    .createLocalFile(m_settings.getFileNameWithSuffix());
            FileHandlingUtility.checkOverwrite(remoteFile, m_settings.getFileOverwritePolicy());

            // For local or HDFS_local connection write directly.
            writeToFile(exec, input, remoteFile, false);
        }
    }

    private void writeTempFilesAndUpload(final ExecutionContext exec, final RowInput input,
            final RemoteFile<Connection> remoteFile) throws Exception {
        final File path = FileUtil.createTempDir("cloudupload");

        boolean createNewFile = true;
        int filecount = 0;

        final FileUploader fileUploader = new FileUploader(m_settings.getNumOfLocalChunks(), remoteFile,
                exec.createSubExecutionContext(0.5), m_settings.getFilenameSuffix());
        final ExecutorService uploadExecutor = Executors.newSingleThreadExecutor();
        final Future<String> resultString = uploadExecutor.submit(fileUploader);

        while (createNewFile) {
            final RemoteFile<Connection> tempFile = FileHandlingUtility.createTempFile(path, filecount);

            // Write to temporary file.
            createNewFile = writeToFile(exec, input, tempFile, true);
            filecount++;
            LOGGER.info(String.format("Written temporary file %s.", tempFile.getFullName()));
            exec.setMessage(String.format("Written temporary file %s.", tempFile.getFullName()));
            if (!createNewFile) {
                fileUploader.setWriteFinished(true);
            }
            fileUploader.addFile(tempFile);
        }
        // wait for file uploader to finish
        final String uploadedFilesMessage = resultString.get();
        LOGGER.info(uploadedFilesMessage);
    }

    private boolean writeToFile(final ExecutionContext exec, final RowInput input,
            final RemoteFile<Connection> remoteFile, final boolean writeChunks) throws Exception {

        exec.setMessage("Starting to write File.");
		final DataTypeMappingService<X, ?, ?> mappingService = m_settings.getFormatFactory().getTypeMappingService();
        final DataTypeMappingConfiguration<X> mappingConfiguration =
        		m_settings.getMappingModel().getDataTypeMappingConfiguration(mappingService);
        try (final AbstractFileFormatWriter writer = createWriter(input, remoteFile, mappingConfiguration)) {

            for (DataRow row; (row = input.poll()) != null;) {

                if (m_rowCountWritten % PROGRESS_UPDATE_ROW_COUNT == 0) {
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
}
