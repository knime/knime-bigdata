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
import java.util.function.Consumer;

import org.apache.commons.lang3.mutable.MutableLong;
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
import org.knime.filehandling.core.defaultnodesettings.filechooser.writer.SettingsModelWriterFileChooser;
import org.knime.filehandling.core.defaultnodesettings.filechooser.writer.WritePathAccessor;
import org.knime.filehandling.core.defaultnodesettings.filtermode.SettingsModelFilterMode.FilterMode;
import org.knime.filehandling.core.defaultnodesettings.status.NodeModelStatusConsumer;
import org.knime.filehandling.core.defaultnodesettings.status.StatusMessage;
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
        m_statusConsumer.setWarningsIfRequired(this::setWarningMessage);
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

    // TODO Framework code?
    private static void createParentDirIfRequired(final FileFormatWriter2Config<?> config, final Path outputPath)
            throws IOException {
        // create parent directories according to the state of m_createDirectoryConfig.
        final Path parentPath = outputPath.getParent();
        if (parentPath != null && !FSFiles.exists(parentPath)) {
            if (config.getFileChooserModel().isCreateMissingFolders()) {
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
    protected BufferedDataTable[] execute(final PortObject[] data, final ExecutionContext exec) throws Exception {
        final BufferedDataTable tbl = (BufferedDataTable)data[m_dataInputPortIdx];
        final DataTableRowInput rowInput = new DataTableRowInput(tbl);
        write(exec, m_writerConfig, m_factory, m_statusConsumer, rowInput, tbl.size());
        return new BufferedDataTable[0];
    }

    @Override
    public StreamableOperator createStreamableOperator(final PartitionInfo partitionInfo,
        final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        return new StreamableOperator() {
            @Override
            public void runFinal(final PortInput[] inputs, final PortOutput[] outputs, final ExecutionContext exec)
                throws Exception {
                final RowInput input = (RowInput)inputs[m_dataInputPortIdx];
                write(exec, m_writerConfig, m_factory, m_statusConsumer, input, -1);
            }
        };
    }

    private static <T> void write(final ExecutionMonitor exec, final FileFormatWriter2Config<T> config,
        final FileFormatFactory<T> factory, final Consumer<StatusMessage> statusConsumer, final RowInput rowInput,
        final long rowCount)
        throws InvalidSettingsException, IOException, CanceledExecutionException, InterruptedException {
        final SettingsModelWriterFileChooser fileChooserModel = config.getFileChooserModel();
        try (final WritePathAccessor accessor = fileChooserModel.createWritePathAccessor()) {
            final FSPath outputPath = accessor.getOutputPath(statusConsumer);
            createParentDirIfRequired(config, outputPath);
            try {
                if (writeMultipleFiles(config)) {
                    writeToDir(exec, config, factory, outputPath, rowInput, rowCount);
                } else {
                    writeToFile(exec, config, factory, outputPath, rowInput, rowCount, new MutableLong(0));
                }
            } catch (FileAlreadyExistsException e) {
                throw new IOException("File '" + outputPath.toString() + "' already exists", e);
            }
        }
    }

    private static <T> void writeToDir(final ExecutionMonitor exec, final FileFormatWriter2Config<T> config,
        final FileFormatFactory<T> factory, final FSPath outputRootPath, final RowInput input, final long rowCount)
                throws InvalidSettingsException, IOException, CanceledExecutionException, InterruptedException {
        if (!FSFiles.exists(outputRootPath)) {
                FSFiles.createDirectories(outputRootPath);
        } else {
            if (FileOverwritePolicy.FAIL == config.getFileChooserModel().getFileOverwritePolicy()) {
                final String msg;
                if (factory.getSupportedPolicies().contains(FileOverwritePolicy.OVERWRITE)) {
                    msg = "Folder already exists and must not be overwritten due to user settings.";
                } else {
                    msg = "Folder already exists and cannot be overwritten.";
                }
                throw new IOException(msg);
            }
        }
        int fileCounter = 0;
        final MutableLong rowCounter = new MutableLong(0);
        boolean moreData = true;
        while (moreData) {
            final String fileName =
                    String.format("%s%05d%s", config.getFileNamePrefix(), fileCounter, factory.getFilenameSuffix());
            final FSPath outputPath = (FSPath) outputRootPath.resolve(fileName);
            moreData = writeToFile(exec, config, factory, outputPath, input, rowCount, rowCounter);
            fileCounter++;
        }
    }

    private static <T> boolean writeToFile(final ExecutionMonitor exec, final FileFormatWriter2Config<T> config,
        final FileFormatFactory<T> factory, final FSPath outputPath, final RowInput input, final long rowCount,
        final MutableLong rowCounter)
        throws InvalidSettingsException, IOException, CanceledExecutionException, InterruptedException {
        final boolean isNewFile = !FSFiles.exists(outputPath);
        final boolean writeMultipleFiles = writeMultipleFiles(config);
        try (final FileFormatWriter formatWriter =
                createWriter(outputPath, input, config, factory)) {
            for (DataRow row; (row = input.poll()) != null;) {
                reportProgress(rowCount, rowCounter, exec);
                boolean writeRow = formatWriter.writeRow(row);
                rowCounter.increment();
                if (writeMultipleFiles && writeRow) {
                    return true;
                }
            }
            return false;
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

    private static <T> boolean writeMultipleFiles(final FileFormatWriter2Config<T> config) {
        return FilterMode.FOLDER == config.getFileChooserModel().getFilterMode();
    }

    private static <T> FileFormatWriter createWriter(final FSPath path, final RowInput input,
        final FileFormatWriter2Config<T> config, final FileFormatFactory<T> factory)
                throws IOException, InvalidSettingsException {
        final FileOverwritePolicy overwritePolicy = config.getFileChooserModel().getFileOverwritePolicy();
        final DataTypeMappingService<T, ?, ?> mappingService = factory.getTypeMappingService();
        final DataTypeMappingConfiguration<T> mappingConfiguration =
                config.getMappingModel().getDataTypeMappingConfiguration(mappingService);
        return factory.getWriter(path, overwritePolicy, input.getDataTableSpec(), config.getFileSize(),
            config.getChunkSize(), config.getSelectedCompression(), mappingConfiguration);
    }

    private static void reportProgress(final long rowCount, final MutableLong rowCounter, final ExecutionMonitor exec)
            throws CanceledExecutionException {
        if (rowCounter.longValue() % PROGRESS_UPDATE_ROW_COUNT == 0) {
            if (rowCount > 0) {
                exec.setProgress(rowCounter.doubleValue() / rowCount,
                    String.format("Written row %d of %d.", rowCounter.longValue(), rowCount));
            } else {
                exec.setProgress(String.format("Written row %d.", rowCounter.longValue()));
            }
        }
        exec.checkCanceled();
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
