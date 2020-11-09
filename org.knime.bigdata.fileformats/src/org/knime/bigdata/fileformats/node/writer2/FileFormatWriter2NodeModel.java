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
package org.knime.bigdata.fileformats.node.writer2;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.EnumSet;

import org.knime.bigdata.fileformats.node.writer.FileFormatWriter;
import org.knime.bigdata.fileformats.utility.FileFormatFactory;
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
import org.knime.core.node.context.ports.PortsConfiguration;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.streamable.DataTableRowInput;
import org.knime.core.node.streamable.InputPortRole;
import org.knime.core.node.streamable.PartitionInfo;
import org.knime.core.node.streamable.PortInput;
import org.knime.core.node.streamable.PortOutput;
import org.knime.core.node.streamable.RowInput;
import org.knime.core.node.streamable.StreamableOperator;
import org.knime.datatype.mapping.DataTypeMappingConfiguration;
import org.knime.datatype.mapping.DataTypeMappingService;
import org.knime.filehandling.core.connections.FSFiles;
import org.knime.filehandling.core.connections.FSPath;
import org.knime.filehandling.core.defaultnodesettings.filechooser.writer.FileOverwritePolicy;
import org.knime.filehandling.core.defaultnodesettings.filechooser.writer.WritePathAccessor;
import org.knime.filehandling.core.defaultnodesettings.status.NodeModelStatusConsumer;
import org.knime.filehandling.core.defaultnodesettings.status.StatusMessage.MessageType;

/*
 * @author Christian Dietz, KNIME GmbH, Konstanz Germany
 */
class FileFormatWriter2NodeModel<T> extends NodeModel {

    private static final int PROGRESS_UPDATE_ROW_COUNT = 100;

    private static final NodeLogger LOGGER = NodeLogger.getLogger(FileFormatWriter2NodeModel.class);

    private final FileFormatWriter2Config<T> m_writerConfig;

    private final NodeModelStatusConsumer m_statusConsumer;

    private final int m_dataInputPortIdx;

    private final FileFormatFactory<T> m_factory;

    FileFormatWriter2NodeModel(final PortsConfiguration config, final FileFormatFactory<T> factory) {
        super(config.getInputPorts(), config.getOutputPorts());
        m_writerConfig = new FileFormatWriter2Config<>(config, factory);
        m_statusConsumer = new NodeModelStatusConsumer(EnumSet.of(MessageType.ERROR, MessageType.WARNING));

        m_factory = factory;

        // save since this port is fixed, see the Factory class
        m_dataInputPortIdx =
            config.getInputPortLocation().get(AbstractFileFormatWriter2NodeFactory.DATA_TABLE_INPUT_PORT_GRP_NAME)[0];
    }

    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        initializeTypeMappingIfNecessary();
        m_writerConfig.getFileChooserModel().configureInModel(inSpecs, m_statusConsumer);
        m_statusConsumer.setWarningsIfRequired(this::setWarningMessage);
        return new DataTableSpec[0];
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_writerConfig.saveSettingsTo(settings);
    }

    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_writerConfig.validateSettings(settings);
    }

    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_writerConfig.loadSettingsFrom(settings);
    }

    @Override
    protected void reset() {
    }

    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
    }

    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
    }

    @Override
    protected BufferedDataTable[] execute(final PortObject[] data, final ExecutionContext exec) throws Exception {
        try (final WritePathAccessor accessor = m_writerConfig.getFileChooserModel().createWritePathAccessor()) {
            final FSPath outputPath = accessor.getOutputPath(m_statusConsumer);
            m_statusConsumer.setWarningsIfRequired(this::setWarningMessage);
            createParentDirIfRequired(outputPath);
            final BufferedDataTable tbl = (BufferedDataTable)data[m_dataInputPortIdx];
            final DataTableRowInput rowInput = new DataTableRowInput(tbl);
            try {
                return writeToFile(rowInput, exec, outputPath);
            } catch (FileAlreadyExistsException e) {
                throw new IOException("File '" + outputPath.toString() + "' already exists", e);
            }
        }
    }

    // TODO Framework code?
    private void createParentDirIfRequired(final Path outputPath) throws IOException {
        // create parent directories according to the state of m_createDirectoryConfig.
        final Path parentPath = outputPath.getParent();
        if (parentPath != null && !FSFiles.exists(parentPath)) {
            if (m_writerConfig.getFileChooserModel().isCreateMissingFolders()) {
                FSFiles.createDirectories(parentPath);
            } else {
                throw new IOException(String.format(
                    "The directory '%s' does not exist and must not be created due to user settings.", parentPath));
            }
        }
    }

    @Override
    public InputPortRole[] getInputPortRoles() {
        final InputPortRole[] inputPortRoles = new InputPortRole[getNrInPorts()];
        // Set all ports except the data table input port to NONSTREAMABLE.
        Arrays.fill(inputPortRoles, InputPortRole.NONDISTRIBUTED_NONSTREAMABLE);
        inputPortRoles[m_dataInputPortIdx] = InputPortRole.NONDISTRIBUTED_STREAMABLE;
        return inputPortRoles;
    }

    @Override
    public StreamableOperator createStreamableOperator(final PartitionInfo partitionInfo,
        final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        return new StreamableOperator() {
            @Override
            public void runFinal(final PortInput[] inputs, final PortOutput[] outputs, final ExecutionContext exec)
                throws Exception {
                try (final WritePathAccessor accessor =
                    m_writerConfig.getFileChooserModel().createWritePathAccessor()) {
                    final FSPath outputPath = accessor.getOutputPath(m_statusConsumer);
                    m_statusConsumer.setWarningsIfRequired(s -> setWarningMessage(s));
                    createParentDirIfRequired(outputPath);
                    final RowInput input = (RowInput)inputs[m_dataInputPortIdx];
                    writeToFile(input, exec, outputPath);
                }
            }
        };
    }

    private BufferedDataTable[] writeToFile(final RowInput input, final ExecutionContext exec, final FSPath outputPath)
        throws InvalidSettingsException, IOException, CanceledExecutionException, InterruptedException {

        exec.setMessage("Starting to write File.");
        final boolean isNewFile = !FSFiles.exists(outputPath);

        final DataTypeMappingService<T, ?, ?> mappingService = m_factory.getTypeMappingService();
        final DataTypeMappingConfiguration<T> mappingConfiguration =
            m_writerConfig.getMappingModel().getDataTypeMappingConfiguration(mappingService);
        final FileOverwritePolicy overwritePolicy = m_writerConfig.getFileChooserModel().getFileOverwritePolicy();
        try (final FileFormatWriter formatWriter =
                createWriter(outputPath, overwritePolicy, input, mappingConfiguration)) {

            long rowsWritten = 0;
            for (DataRow row; (row = input.poll()) != null;) {

                if (rowsWritten % PROGRESS_UPDATE_ROW_COUNT == 0) {
                    exec.setProgress(String.format("Written row %d.", rowsWritten));
                }
                formatWriter.writeRow(row);
                rowsWritten++;
                exec.checkCanceled();
            }

            return new BufferedDataTable[0];
        } catch (final CanceledExecutionException e) {
            if (isNewFile) {
                deleteIncompleteFile(outputPath);
            } else {
                LOGGER.warn(
                    "Node exection was canceled. The file '" + outputPath + "' could have partial modifications.");
            }
            throw e;
        }
    }

    private FileFormatWriter createWriter(final FSPath path, final FileOverwritePolicy overwritePolicy,
        final RowInput input, final DataTypeMappingConfiguration<T> mappingConfiguration) throws IOException {
        return m_factory.getWriter(path, overwritePolicy, input.getDataTableSpec(), m_writerConfig.getChunkSize(),
            m_writerConfig.getSelectedCompression(), mappingConfiguration);
    }

    private static void deleteIncompleteFile(final Path outputPath) {
        try {
            Files.delete(outputPath);
            LOGGER.debug("File created '" + outputPath + "' deleted after node execution was canceled.");

        } catch (final IOException ex) {
            LOGGER.warn("Unable to delete created file '" + outputPath + "' after node execution was canceled. "
                + ex.getMessage(), ex);
        }
    }

    /*
     * Ensures that the default type mapping of the give {@link DataTypeMappingService} is copied into the
     * node models {@link DataTypeMappingConfiguration}s if they are empty which is the case when a new node is created.
     * The node dialog itself prevents the user from removing all type mappings so they can only be empty during
     * init.
     */
    private void initializeTypeMappingIfNecessary() {
        try {
            // TODO get rid of parquet
            final DataTypeMappingService<T, ?, ?> mappingService = m_factory.getTypeMappingService();
            final DataTypeMappingConfiguration<T> origExternalToKnimeConf =
                m_writerConfig.getMappingModel().getDataTypeMappingConfiguration(mappingService);
            if (origExternalToKnimeConf.getTypeRules().isEmpty() && origExternalToKnimeConf.getNameRules().isEmpty()) {
                final DataTypeMappingConfiguration<T> combinedConf =
                    origExternalToKnimeConf.with(mappingService.newDefaultKnimeToExternalMappingConfiguration());
                m_writerConfig.getMappingModel().setDataTypeMappingConfiguration(combinedConf);
            }
        } catch (final InvalidSettingsException e) {
            LOGGER.warn("Could not initialize type mapping with default rules.", e);
        }
    }
}
