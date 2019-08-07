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
 *   18.06.2019 (Mareike Hoeger, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.bigdata.database.loader;

import static java.nio.file.Files.delete;
import static java.util.Arrays.asList;
import static org.knime.base.filehandling.NodeUtils.encodePath;
import static org.knime.base.filehandling.NodeUtils.resetGBC;
import static org.knime.base.filehandling.remote.files.RemoteFileFactory.createRemoteFile;
import static org.knime.core.util.FileUtil.createTempFile;
import static org.knime.database.agent.metadata.DBMetaDataHelper.createDBTableSpec;
import static org.knime.database.util.RemoteFiles.copyLocalFile;
import static org.knime.database.util.RemoteFiles.resolveFileName;
import static org.knime.datatype.mapping.DataTypeMappingDirection.KNIME_TO_EXTERNAL;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.sql.SQLType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JLabel;
import javax.swing.JPanel;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.base.filehandling.remote.files.RemoteFileFactory;
import org.knime.bigdata.fileformats.node.writer.AbstractFileFormatWriter;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetType;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetTypeMappingService;
import org.knime.bigdata.fileformats.parquet.writer.ParquetKNIMEWriter;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.container.ColumnRearranger;
import org.knime.core.data.convert.map.ConsumptionPath;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.streamable.RowInput;
import org.knime.database.DBTableSpec;
import org.knime.database.agent.loader.DBLoadTableFromFileParameters;
import org.knime.database.agent.loader.DBLoader;
import org.knime.database.agent.metadata.DBMetadataReader;
import org.knime.database.agent.metadata.impl.DefaultDBTableSpec;
import org.knime.database.datatype.mapping.DBTypeMappingRegistry;
import org.knime.database.datatype.mapping.DBTypeMappingService;
import org.knime.database.dialect.DBColumn;
import org.knime.database.model.DBTable;
import org.knime.database.model.impl.DefaultDBColumn;
import org.knime.database.node.io.load.impl.ConnectableCsvLoaderNodeSettings;
import org.knime.database.node.io.load.impl.ConnectedCsvLoaderNodeComponents;
import org.knime.database.node.io.load.impl.ConnectedLoaderNode;
import org.knime.database.port.DBDataPortObject;
import org.knime.database.port.DBDataPortObjectSpec;
import org.knime.database.port.DBSessionPortObject;
import org.knime.database.port.DBSessionPortObjectSpec;
import org.knime.database.session.DBSession;
import org.knime.datatype.mapping.DataTypeMappingConfiguration;
import org.knime.datatype.mapping.DataTypeMappingDirection;
import org.knime.node.datatype.mapping.DataTypeMappingConfigurationData;

/**
 * Class for Big Data Loader node
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class BigDataLoaderNode
    extends ConnectedLoaderNode<ConnectedCsvLoaderNodeComponents, ConnectableCsvLoaderNodeSettings> {

    /**
     * LOGGER for Big Data Loader nodes
     */
    private static final NodeLogger LOGGER = NodeLogger.getLogger(BigDataLoaderNode.class);

    private static Box createBox(final boolean horizontal) {
        final Box box;
        if (horizontal) {
            box = new Box(BoxLayout.X_AXIS);
            box.add(Box.createVerticalGlue());
        } else {
            box = new Box(BoxLayout.Y_AXIS);
            box.add(Box.createHorizontalGlue());
        }
        return box;
    }

    private static JPanel createPanel() {
        final JPanel panel = new JPanel();
        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));
        return panel;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void buildDialog(final DialogBuilder builder, final List<DialogComponent> dialogComponents,
        final ConnectedCsvLoaderNodeComponents customComponents) {
        final JPanel optionsPanel = createPanel();
        final Box optionsBox = createBox(false);
        optionsPanel.add(optionsBox);
        // Table name
        optionsBox.add(customComponents.getTableNameComponent().getComponentPanel());
        // Target folder
        final JPanel fileChooserPanel = new JPanel(new GridBagLayout());
        final GridBagConstraints constraints = new GridBagConstraints();
        resetGBC(constraints);
        constraints.anchor = GridBagConstraints.WEST;
        fileChooserPanel.add(new JLabel("Target folder: "), constraints);
        constraints.fill = GridBagConstraints.HORIZONTAL;
        constraints.gridx = 1;
        constraints.weightx = 1;
        fileChooserPanel.add(customComponents.getTargetFolderComponent().getComponentPanel(), constraints);
        optionsBox.add(fileChooserPanel);
        builder.addTab(Integer.MAX_VALUE, "Options", optionsPanel, true);
    }

    @Override
    protected DBDataPortObjectSpec configureModel(final PortObjectSpec[] inSpecs,
        final List<SettingsModel> settingsModels, final ConnectableCsvLoaderNodeSettings customSettings)
        throws InvalidSettingsException {

        final DBSessionPortObjectSpec sessionPortObjectSpec = (DBSessionPortObjectSpec)inSpecs[1];
        final DBSession session = sessionPortObjectSpec.getDBSession();

        final ExecutionMonitor exec = createModelConfigurationExecutionMonitor(session);
        final DataTableSpec tableSpecification = (DataTableSpec)inSpecs[0];

        ColumnListsProvider columnListProvider;
        try {
            columnListProvider = new ColumnListsProvider(exec, customSettings.getTableNameModel().toDBTable(), session,
                tableSpecification);
        } catch (final Exception ex) {
            throw new InvalidSettingsException(ex.getMessage());
        }

        validateColumns(tableSpecification, sessionPortObjectSpec.getKnimeToExternalTypeMapping(), session,
            columnListProvider.getNormalColumns(), columnListProvider.getPartitionColumns());

        return super.configureModel(inSpecs, settingsModels, customSettings);
    }

    private static DataTableSpec validateColumns(final DataTableSpec tableSpecification,
        final DataTypeMappingConfigurationData dataTypeMappingConfigurationData, final DBSession session,
        final List<DBColumn> normalColumns, final List<DBColumn> partitionColumns) throws InvalidSettingsException {

        //Create DefaultDBColums from org.knime.database.dialect.DBColumn
        final DefaultDBColumn[] cols = Stream.concat(normalColumns.stream(), partitionColumns.stream())
            .map(t -> new DefaultDBColumn(t.getName(), DBtoParquetTypeUtil.dbToJDBCType(t.getType()), t.getType()))
            .toArray((DefaultDBColumn[]::new));

        final DBTableSpec databaseTableSpecification = new DefaultDBTableSpec(cols);
        DataTableSpec rearrangedSpec = tableSpecification;
        if (!partitionColumns.isEmpty()) {
            rearrangedSpec = rearrangeColumns(tableSpecification, partitionColumns);
        }

        final ConsumptionPath[] consumptionPaths = dataTypeMappingConfigurationData
            .resolve(DBTypeMappingRegistry.getInstance().getDBTypeMappingService(session.getDBType()),
                KNIME_TO_EXTERNAL)
            .getConsumptionPathsFor(rearrangedSpec);
        final org.knime.database.model.DBColumn[] columns = createDBTableSpec(rearrangedSpec, consumptionPaths);

        validateColumns(true, columns, databaseTableSpecification);
        return rearrangedSpec;
    }

    /**
     * @param tableSpecification input table specification
     * @param partitionColumns name of the partitions columns
     * @return input table spec rearranged in a way that the partition columns come last
     * @throws InvalidSettingsException if not all partition columns are available in the input table spec
     */
    private static DataTableSpec rearrangeColumns(final DataTableSpec tableSpecification,
        final List<DBColumn> partitionColumns) throws InvalidSettingsException {

        //Create a map for the partition columns with the indices they will end up in
        final Map<String, Integer> partitionMap = new HashMap<>();
        for (int i = 0; i < partitionColumns.size(); i++) {
            partitionMap.put(partitionColumns.get(i).getName(), i);
        }
        //This will hold the names of the KNIME columns, for the rearrange step
        final String[] rearranges = new String[partitionColumns.size()];

        final String[] columnNames = tableSpecification.getColumnNames();
        for (final String col : columnNames) {
            //Check for ever KNIME column if it equals ignore case
            for (final String pCol : partitionMap.keySet()) {
                if (pCol.equalsIgnoreCase(col)) {
                    //Found a partition column equivalent, save the KNIME table name
                    // and remove the partitioning column from the lookup set
                    rearranges[partitionMap.get(pCol)] = col;
                    partitionMap.remove(pCol);
                    break;
                }
            }
            if (partitionMap.isEmpty()) {
                //Found all partitioning columns
                break;
            }
        }

        if (!partitionMap.isEmpty()) {
            throw new InvalidSettingsException(
                String.format("The input table is missing the partitioning columns %s", partitionMap.keySet()));
        }

        final ColumnRearranger rearranger = new ColumnRearranger(tableSpecification);
        for (final String col : rearranges) {
            rearranger.move(col, rearranger.getColumnCount());
        }
        return rearranger.createSpec();
    }

    @Override
    protected ConnectedCsvLoaderNodeComponents createCustomDialogComponents(final DialogDelegate dialogDelegate) {
        return new ConnectedCsvLoaderNodeComponents(dialogDelegate);
    }

    @Override
    protected ConnectableCsvLoaderNodeSettings createCustomModelSettings(final ModelDelegate modelDelegate) {
        return new ConnectableCsvLoaderNodeSettings(modelDelegate);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected List<DialogComponent> createDialogComponents(final ConnectedCsvLoaderNodeComponents customComponents) {
        return asList(customComponents.getTargetFolderComponent(), customComponents.getTableNameComponent());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected List<SettingsModel> createSettingsModels(final ConnectableCsvLoaderNodeSettings customSettings) {
        return asList(customSettings.getTargetFolderModel(), customSettings.getTableNameModel());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected DBDataPortObject load(final ExecutionParameters<ConnectableCsvLoaderNodeSettings> parameters)
        throws Exception {
        final DBSessionPortObject sessionPortObject = parameters.getSessionPortObject();
        final DBSession session = sessionPortObject.getDBSession();
        final ExecutionContext exec = parameters.getExecutionContext();
        final ConnectableCsvLoaderNodeSettings customSettings = parameters.getCustomSettings();
        final DBTable table = customSettings.getTableNameModel().toDBTable();
        final DataTableSpec tableSpec = parameters.getRowInput().getDataTableSpec();
        final ConnectionMonitor<?> connectionMonitor = new ConnectionMonitor<>();
        final ColumnListsProvider columnLists = new ColumnListsProvider(exec, table, session, tableSpec);

        // Build and validate columns lists.
        validateColumns(tableSpec, sessionPortObject.getKnimeToExternalTypeMapping(), session,
            columnLists.getNormalColumns(), columnLists.getPartitionColumns());

        //Write local temporary file
        final Path temporaryFile = getTempFilePath();
        delete(temporaryFile);
        try (AutoCloseable temporaryFileDeleter = () -> delete(temporaryFile);
                AutoCloseable connectionMonitorCloser = () -> connectionMonitor.closeAll()) {

            writeTemporaryFile(parameters.getRowInput(), temporaryFile, columnLists.getTempTableColumns(), exec);

            final RemoteFile<?> targetFile =
                getTargetFile(parameters, customSettings, temporaryFile, connectionMonitor);

            try (AutoCloseable targetFileDeleter = new RemoteFileDeleter(customSettings, targetFile)) {
                uploadTemporaryFile(exec, temporaryFile, targetFile);

                // Load the data
                session.getAgent(DBLoader.class).load(exec,
                    new DBLoadTableFromFileParameters<BigDataLoaderParameters>(null, targetFile.getFullName(), table,
                        new BigDataLoaderParameters(columnLists.getPartitionColumns(),
                            columnLists.getTempTableColumns(), columnLists.getSelectOrderColumnNames(), targetFile)));
            }
        }

        // Output
        return new DBDataPortObject(sessionPortObject, session.getAgent(DBMetadataReader.class).getDBDataObject(exec,
            table.getSchemaName(), table.getName(), getExternalToKnimeTypeMapping(sessionPortObject, session)));
    }

    private static DataTypeMappingConfiguration<SQLType> getExternalToKnimeTypeMapping(
        final DBSessionPortObject sessionPortObject, final DBSession session) throws InvalidSettingsException {
        final DBTypeMappingService<?, ?> typeMappingService =
            DBTypeMappingRegistry.getInstance().getDBTypeMappingService(session.getDBType());

        return sessionPortObject.getExternalToKnimeTypeMapping().resolve(typeMappingService,
            DataTypeMappingDirection.EXTERNAL_TO_KNIME);
    }

    /**
     *  Deleter class for try-with-resources
     *
     * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
     */
    private static class RemoteFileDeleter implements AutoCloseable {

        private final ConnectableCsvLoaderNodeSettings m_customSettings;

        private final RemoteFile<?> m_targetFile;

        RemoteFileDeleter(final ConnectableCsvLoaderNodeSettings customSettings, final RemoteFile<?> targetFile) {
            m_customSettings = customSettings;
            m_targetFile = targetFile;
        }

        @Override
        public void close() throws Exception {
            if (m_targetFile.exists() && !m_targetFile.delete()) {
                m_customSettings.getModelDelegate().setWarning("The target file could not be deleted.");
            }
        }
    }

    private static void uploadTemporaryFile(final ExecutionContext exec, final Path temporaryFile,
        final RemoteFile<?> targetFile) throws Exception {
        LOGGER.debugWithFormat("Target file URI: \"%s\"", targetFile.getURI());
        copyLocalFile(temporaryFile, targetFile, exec);
        exec.setProgress(0.5, "File uploaded.");
        LOGGER.debug("The content has been copied to the target file.");
    }

    private static RemoteFile<?> getTargetFile(final ExecutionParameters<ConnectableCsvLoaderNodeSettings> parameters,
        final ConnectableCsvLoaderNodeSettings customSettings, final Path temporaryFile,
        final ConnectionMonitor<?> connectionMonitor) throws Exception {
        String targetFolder = customSettings.getTargetFolderModel().getStringValue();
        if (!targetFolder.endsWith("/")) {
            targetFolder += '/';
        }
        final ConnectionInformation connectionInformation =
            parameters.getConnectionInformationPortObject().getConnectionInformation();
        final RemoteFile<?> targetDirectory =
            createRemoteFile(new URI(connectionInformation.toURI() + encodePath(targetFolder)), connectionInformation,
                connectionMonitor);
        LOGGER.debugWithFormat("Target folder URI: \"%s\"", targetDirectory.getURI());
        return resolveFileName(targetDirectory, temporaryFile);
    }

    private static void writeTemporaryFile(final RowInput rowInput, final Path temporaryFile,
        final DBColumn[] tempTableColumns, final ExecutionContext exec) throws Exception {
        exec.checkCanceled();
        exec.setMessage("Writing temporary file...");
        final RemoteFile<Connection> file = RemoteFileFactory.createRemoteFile(temporaryFile.toUri(), null, null);
        final DataTableSpec spec = rowInput.getDataTableSpec();

        try (AbstractFileFormatWriter writer = createWriter(file, spec, tempTableColumns)) {
            DataRow row;
            while ((row = rowInput.poll()) != null) {
                writer.writeRow(row);
            }
        } finally {
            rowInput.close();
            LOGGER.debug("Written file " + temporaryFile + " ");
        }
        exec.setProgress(0.25, "Temporary file written.");
        exec.checkCanceled();
    }

    /**
     * Creates a Path for the temporary file to write
     *
     * @return temporary file Path
     * @throws IOException
     */
    protected Path getTempFilePath() throws IOException {
        // Create and write to the temporary file
        return createTempFile("knime2db", ".parquet").toPath();
    }

    /**
     * Creates a {@link AbstractFileFormatWriter}
     *
     * @param file the file to write to
     * @param spec the DataTableSpec of the input
     * @param tempTableColumns List of columns in order of input table, renamed to generic names
     * @return AbstractFileFormatWriter
     * @throws IOException if writer cannot be initialized
     */
    private static AbstractFileFormatWriter createWriter(final RemoteFile<Connection> file, final DataTableSpec spec,
        final DBColumn[] tempTableColumns) throws IOException {
        final DataTableSpec newSpec = createRenamedSpec(spec, tempTableColumns);
        return new ParquetKNIMEWriter(file, newSpec, "UNCOMPRESSED", -1,
            getParquetTypesMapping(spec, tempTableColumns));
    }

    private static DataTypeMappingConfiguration<ParquetType> getParquetTypesMapping(final DataTableSpec spec,
        final DBColumn[] inputColumns) {

        final List<ParquetType> parquetTypes = mapDBToParquetTypes(inputColumns);

        final DataTypeMappingConfiguration<ParquetType> configuration = ParquetTypeMappingService.getInstance()
            .createMappingConfiguration(DataTypeMappingDirection.KNIME_TO_EXTERNAL);

        for (int i = 0; i < spec.getNumColumns(); i++) {
            final DataColumnSpec knimeCol = spec.getColumnSpec(i);
            final DataType dataType = knimeCol.getType();
            final ParquetType parquetType = parquetTypes.get(i);
            final Collection<ConsumptionPath> consumPaths =
                ParquetTypeMappingService.getInstance().getConsumptionPathsFor(dataType);

            final Optional<ConsumptionPath> path = consumPaths.stream()
                .filter(p -> p.getConsumerFactory().getDestinationType().equals(parquetType)).findFirst();
            if (path.isPresent()) {
                configuration.addRule(dataType, path.get());
            } else {
                final String error =
                    String.format("Could not find ConsumptionPath for %s to JDBC Type %s via Parquet Type %s", dataType,
                        inputColumns[i].getType(), parquetType);
                LOGGER.error(error);
                throw new RuntimeException(error);
            }
        }

        return configuration;
    }

    private static List<ParquetType> mapDBToParquetTypes(final DBColumn[] inputColumns) {
        final List<ParquetType> parquetTypes = new ArrayList<>();
        for (final DBColumn dbCol : inputColumns) {
            final String type = dbCol.getType();
            final ParquetType parquetType = DBtoParquetTypeUtil.dbToParquetType(type);
            if (parquetType == null) {
                throw new RuntimeException(String.format("Cannot find Parquet type for Database type %s", type));
            }
            parquetTypes.add(parquetType);
        }
        return parquetTypes;
    }

    /**
     * Renames all columns in the DataTableSpec to the the column names of the temporary table
     *
     * @param inputTableSpec the DataTablespec to rename
     * @param tempTableColumns the column list for the temporary table
     * @return the renamed DataTableSpec
     */
    private static DataTableSpec createRenamedSpec(final DataTableSpec inputTableSpec,
        final DBColumn[] tempTableColumns) {
        final DataColumnSpec[] cols = new DataColumnSpec[inputTableSpec.getNumColumns()];
        for (int i = 0; i < cols.length; i++) {
            final DataColumnSpec oldCol = inputTableSpec.getColumnSpec(i);
            final DataColumnSpecCreator creator = new DataColumnSpecCreator(oldCol);
            creator.setName(tempTableColumns[i].getName());
            cols[i] = creator.createSpec();
        }
        return new DataTableSpec(inputTableSpec.getName(), cols);
    }
}
