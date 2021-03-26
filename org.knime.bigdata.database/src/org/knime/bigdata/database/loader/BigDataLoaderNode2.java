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

import static java.util.Arrays.asList;
import static org.knime.database.agent.metadata.DBMetaDataHelper.createDBTableSpec;
import static org.knime.datatype.mapping.DataTypeMappingDirection.KNIME_TO_EXTERNAL;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import javax.swing.JPanel;

import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.eclipse.core.runtime.URIUtil;
import org.knime.bigdata.fileformats.parquet.ParquetFileFormatWriter;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetType;
import org.knime.bigdata.fileformats.parquet.datatype.mapping.ParquetTypeMappingService;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.container.ColumnRearranger;
import org.knime.core.data.convert.map.ConsumptionPath;
import org.knime.core.node.CanceledExecutionException;
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
import org.knime.database.agent.loader.DBLoaderMode;
import org.knime.database.agent.metadata.DBMetadataReader;
import org.knime.database.agent.metadata.impl.DefaultDBTableSpec;
import org.knime.database.connection.DBConnectionManagerAttributes;
import org.knime.database.datatype.mapping.DBTypeMappingRegistry;
import org.knime.database.dialect.DBColumn;
import org.knime.database.model.DBTable;
import org.knime.database.model.impl.DefaultDBColumn;
import org.knime.database.node.io.load.DBLoaderNode2;
import org.knime.database.node.io.load.DBLoaderNode2Factory;
import org.knime.database.node.io.load.ExecutionParameters;
import org.knime.database.node.io.load.impl.fs.ConnectableCsvLoaderNodeSettings2;
import org.knime.database.node.io.load.impl.fs.ConnectedCsvLoaderNodeComponents2;
import org.knime.database.node.io.load.impl.fs.ConnectedLoaderNode2;
import org.knime.database.node.io.load.impl.fs.DBFileLoadUtil;
import org.knime.database.node.io.load.impl.fs.DBFileLoadUtil.DBFileLoader;
import org.knime.database.port.DBDataPortObjectSpec;
import org.knime.database.port.DBPortObject;
import org.knime.database.session.DBSession;
import org.knime.datatype.mapping.DataTypeMappingConfiguration;
import org.knime.datatype.mapping.DataTypeMappingDirection;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.connections.FSPath;
import org.knime.filehandling.core.connections.uriexport.URIExporter;
import org.knime.filehandling.core.connections.uriexport.URIExporterIDs;
import org.knime.filehandling.core.connections.uriexport.noconfig.NoConfigURIExporterFactory;
import org.knime.filehandling.core.defaultnodesettings.filechooser.writer.SettingsModelWriterFileChooser;
import org.knime.node.datatype.mapping.DataTypeMappingConfigurationData;

/**
 * Class for Big Data Loader node
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public class BigDataLoaderNode2
    extends ConnectedLoaderNode2<ConnectedCsvLoaderNodeComponents2, ConnectableCsvLoaderNodeSettings2>
    implements DBLoaderNode2Factory<ConnectedCsvLoaderNodeComponents2, ConnectableCsvLoaderNodeSettings2> {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(BigDataLoaderNode2.class);

    private static final int TO_BYTE = 1024 * 1024;

    private static final long FILE_SIZE = TO_BYTE * 1024l;

    private static final int ROW_GROUP_SIZE = ParquetWriter.DEFAULT_BLOCK_SIZE * TO_BYTE;

    private static final class DBLoaderFileWriterImplementation
        implements DBFileLoader<ConnectableCsvLoaderNodeSettings2, BigDataLoaderParameters2> {
        private final TempTableColumnListProvider m_tempColumnLists;

        private final ColumnListsProvider m_columnLists;

        private DBLoaderFileWriterImplementation(final TempTableColumnListProvider tempColumnLists,
            final ColumnListsProvider columnLists) {
            m_tempColumnLists = tempColumnLists;
            m_columnLists = columnLists;
        }

        @Override
        public String getFileExtension() {
            return ".parquet";
        }

        @Override
        public BigDataLoaderParameters2 getLoadParameter(final ConnectableCsvLoaderNodeSettings2 settings) {
            return new BigDataLoaderParameters2(m_columnLists.getPartitionColumns(),
                m_tempColumnLists.getTempTableColumns(), m_tempColumnLists.getSelectOrderColumnNames());
        }

        @Override
        public void writerAndLoad(final ExecutionParameters<ConnectableCsvLoaderNodeSettings2> parameters,
            final ExecutionMonitor exec, final FSPath targetFile, final DBLoaderMode mode,
            final DBSession session, final DBTable table)
            throws Exception {
            exec.setMessage("Writing parquet file...");
            final RowInput rowInput = parameters.getRowInput();
            final DataTableSpec spec = rowInput.getDataTableSpec();
            final ConnectableCsvLoaderNodeSettings2 customSettings = parameters.getCustomSettings();
            final SettingsModelWriterFileChooser targetFolderModel = customSettings.getTargetFolderModel();
            try (ParquetFileFormatWriter writer =
                    createWriter(targetFile, spec, m_tempColumnLists.getTempTableColumns())) {
                DataRow row;
                while ((row = rowInput.poll()) != null) {
                    writer.writeRow(row);
                }
                LOGGER.debug("Written file " + targetFile + " ");
                exec.setProgress(0.25, "File written.");
                exec.checkCanceled();
            } finally {
                rowInput.close();
            }
            try (FSConnection connection = targetFolderModel.getConnection()) {
                final NoConfigURIExporterFactory uriExporterFactory =
                    (NoConfigURIExporterFactory) connection.getURIExporterFactories().get(URIExporterIDs.DEFAULT_HADOOP);
                final URIExporter uriExporter = uriExporterFactory.getExporter();
                final String targetFileString = URIUtil.toUnencodedString(uriExporter.toUri(targetFile));
                exec.setProgress("Loading data file into DB table...");
                exec.checkCanceled();
                session.getAgent(DBLoader.class).load(exec, new DBLoadTableFromFileParameters<>(mode, targetFileString,
                        table, getLoadParameter(customSettings)));
            }
        }

        @Override
        public void writerMethod(final ExecutionParameters<ConnectableCsvLoaderNodeSettings2> parameters,
            final ExecutionMonitor executionContext, final OutputStream outputStream)
            throws CanceledExecutionException, IOException {
            throw new IllegalStateException("Shouldn't be called");
        }

        /**
         * Creates a {@link ParquetFileFormatWriter}
         *
         * @param file the file to write to
         * @param spec the DataTableSpec of the input
         * @param tempTableColumns List of columns in order of input table, renamed to generic names
         * @return ParquetFileFormatWriter to use
         * @throws IOException if writer cannot be initialized
         */
        private static ParquetFileFormatWriter createWriter(final FSPath file, final DataTableSpec spec,
            final DBColumn[] tempTableColumns) throws IOException {
            final CompressionCodecName compression = CompressionCodecName.UNCOMPRESSED;
            final DataTableSpec newSpec = createRenamedSpec(spec, tempTableColumns);
            return new ParquetFileFormatWriter(file, Mode.OVERWRITE, newSpec, compression, FILE_SIZE, ROW_GROUP_SIZE,
                getParquetTypesMapping(spec, tempTableColumns));
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
                    throw new IllegalStateException(error);
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
                    throw new IllegalStateException(String.format("Cannot find Parquet type for Database type %s", type));
                }
                parquetTypes.add(parquetType);
            }
            return parquetTypes;
        }
    }

    @Override
    public DBLoaderNode2<ConnectedCsvLoaderNodeComponents2, ConnectableCsvLoaderNodeSettings2> get() {
        return new BigDataLoaderNode2();
    }

    @Override
    public void buildDialog(final DialogBuilder builder, final List<DialogComponent> dialogComponents,
        final ConnectedCsvLoaderNodeComponents2 customComponents) {
        final JPanel optionsPanel = createTargetTableFolderPanel(customComponents);
        builder.addTab(Integer.MAX_VALUE, "Options", optionsPanel, true);
    }

    @Override
    public DBDataPortObjectSpec configureModel(final PortObjectSpec[] inSpecs,
        final List<SettingsModel> settingsModels, final ConnectableCsvLoaderNodeSettings2 customSettings)
        throws InvalidSettingsException {

        final DBPortObject sessionPortObjectSpec = getDBSpec(inSpecs);
        final DBSession session = sessionPortObjectSpec.getDBSession();

        if (session.getAttributeValues().get(DBConnectionManagerAttributes.ATTRIBUTE_METADATA_IN_CONFIGURE_ENABLED)) {

            final ExecutionMonitor exec = createModelConfigurationExecutionMonitor(session);
            final DataTableSpec tableSpecification = getDataSpec(inSpecs);
            final DBTable dbTable = customSettings.getTableNameModel().toDBTable();

            try {
                buildAndValidateColumnLists(session, exec, dbTable, tableSpecification,
                    sessionPortObjectSpec.getKnimeToExternalTypeMapping());

            } catch (final InvalidSettingsException e) {
                throw e;
            } catch (final Exception ex) {
                throw new InvalidSettingsException(ex);
            }
        }

        return super.configureModel(inSpecs, settingsModels, customSettings);
    }

    private static void validateColumns(final DataTableSpec tableSpecification,
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
    public ConnectedCsvLoaderNodeComponents2 createCustomDialogComponents(final DialogDelegate dialogDelegate) {
        return new ConnectedCsvLoaderNodeComponents2(dialogDelegate);
    }

    @Override
    public ConnectableCsvLoaderNodeSettings2 createCustomModelSettings(final ModelDelegate modelDelegate) {
        return new ConnectableCsvLoaderNodeSettings2(modelDelegate);
    }

    @Override
    public List<DialogComponent> createDialogComponents(final ConnectedCsvLoaderNodeComponents2 customComponents) {
        return asList(customComponents.getTargetFolderComponent(), customComponents.getTableNameComponent());
    }

    @Override
    public void onCloseInDialog(final ConnectedCsvLoaderNodeComponents2 customComponents) {
        super.onCloseInDialog(customComponents);
        customComponents.getTargetFolderComponent().onClose();
    }

    @Override
    public List<SettingsModel> createSettingsModels(final ConnectableCsvLoaderNodeSettings2 customSettings) {
        return asList(customSettings.getTargetFolderModel(), customSettings.getTableNameModel());
    }

    @Override
    public DBTable load(final ExecutionParameters<ConnectableCsvLoaderNodeSettings2> parameters)
        throws Exception {
        final DBPortObject sessionPortObject = parameters.getDBPortObject();
        final DBSession session = sessionPortObject.getDBSession();
        final ExecutionMonitor exec = parameters.getExecutionMonitor();
        final ConnectableCsvLoaderNodeSettings2 customSettings = parameters.getCustomSettings();
        final DBTable table = customSettings.getTableNameModel().toDBTable();
        final DataTableSpec tableSpec = parameters.getRowInput().getDataTableSpec();
        final DataTypeMappingConfigurationData knimeToExternalTypeMapping =
            sessionPortObject.getKnimeToExternalTypeMapping();

        final ColumnListsProvider columnLists =
            buildAndValidateColumnLists(session, exec, table, tableSpec, knimeToExternalTypeMapping);

        final TempTableColumnListProvider tempColumnLists = new TempTableColumnListProvider(
            columnLists.getNormalColumns(), columnLists.getPartitionColumns(), tableSpec);

        //Write the file
        DBFileLoadUtil.writeAndLoadFile(exec, parameters, null, session, table,
            new DBLoaderFileWriterImplementation(tempColumnLists, columnLists));

        // Output
        return table;
    }

    private static ColumnListsProvider buildAndValidateColumnLists(final DBSession session, final ExecutionMonitor exec,
        final DBTable table, final DataTableSpec tableSpec,
        final DataTypeMappingConfigurationData knimeToExternalTypeMapping) throws Exception {

        final boolean tableExist = session.getAgent(DBMetadataReader.class).isExistingTable(exec, table);
        if (!tableExist) {
            throw new InvalidSettingsException(
                String.format("Table %s does not exist", session.getDialect().createFullName(table)));
        }

        final ColumnListsProvider columnLists = new ColumnListsProvider(exec, table, session);
        BigDataLoaderNode2.validateColumns(tableSpec, knimeToExternalTypeMapping, session,
            columnLists.getNormalColumns(), columnLists.getPartitionColumns());
        return columnLists;
    }
}
