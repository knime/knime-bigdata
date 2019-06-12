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
 *   07.05.2019 (Mareike Hoeger, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.bigdata.database.hive.node.loader;

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
import java.net.URI;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JLabel;
import javax.swing.JPanel;

import org.apache.orc.TypeDescription;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.base.filehandling.remote.files.RemoteFileFactory;
import org.knime.bigdata.fileformats.orc.datatype.mapping.ORCTypeMappingService;
import org.knime.bigdata.fileformats.orc.writer.OrcKNIMEWriter;
import org.knime.core.data.DataColumnSpec;
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
 * Implementation of the loader node for the Hive database.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class HiveLoaderNode extends ConnectedLoaderNode<ConnectedCsvLoaderNodeComponents, ConnectableCsvLoaderNodeSettings> {
    private static final NodeLogger LOGGER = NodeLogger.getLogger(HiveLoaderNode.class);

    private DBSessionPortObject m_sessionPortObject;

    private DBTypeMappingService<?, ?> m_typeMappingService;


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

        final List<DBColumn> normalColumns = new ArrayList<>();
        final List<DBColumn> partitionColumns = new ArrayList<>();
        final ExecutionMonitor exec = createModelConfigurationExecutionMonitor(session);
        try {
            fillColumnLists(exec, customSettings.getTableNameModel().toDBTable(), normalColumns, partitionColumns,
                session);
        } catch (Exception ex) {
            throw new InvalidSettingsException(ex.getMessage());
        }
        final DataTableSpec tableSpecification = (DataTableSpec)inSpecs[0];
        validateColumns(tableSpecification, sessionPortObjectSpec.getKnimeToExternalTypeMapping(), session,
            normalColumns, partitionColumns);

        return super.configureModel(inSpecs, settingsModels, customSettings);

    }

    private static void validateColumns(final DataTableSpec tableSpecification,
        final DataTypeMappingConfigurationData dataTypeMappingConfigurationData, final DBSession session,
        final List<DBColumn> normalColumns, final List<DBColumn> partitionColumns)
        throws InvalidSettingsException {
        //Create DefualtDBColums from org.knime.database.dialect.DBColumn

        DefaultDBColumn[] cols = Stream.concat(normalColumns.stream(), partitionColumns.stream())
            .map(t -> new DefaultDBColumn(t.getName(), HiveTypeUtil.hivetoJDBCType(t.getType()), t.getType()))
            .toArray((DefaultDBColumn[]::new));

        final DBTableSpec databaseTableSpecification = new DefaultDBTableSpec(cols);
        DataTableSpec rearrangedSpec = tableSpecification;
        if (!partitionColumns.isEmpty()) {
            rearrangedSpec = rearrangeColumns(tableSpecification, partitionColumns);
        }

        final org.knime.database.model.DBColumn[] columns = createDBTableSpec(rearrangedSpec,
            dataTypeMappingConfigurationData
                .resolve(DBTypeMappingRegistry.getInstance().getDBTypeMappingService(session.getDBType()),
                    KNIME_TO_EXTERNAL).getConsumptionPathsFor(rearrangedSpec));

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
        for (String col : columnNames) {
            //Check for ever KNIME column if it equals ignore case
            for (String pCol : partitionMap.keySet()) {
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
        for (String col : rearranges) {
            rearranger.move(col, rearranger.getColumnCount());
        }
        final DataTableSpec rearrangedSpec = rearranger.createSpec();
        return rearrangedSpec;
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
    protected DBDataPortObject load(final ExecutionParameters<ConnectableCsvLoaderNodeSettings> parameters) throws Exception {

        m_sessionPortObject = parameters.getSessionPortObject();
        final DBSession session = m_sessionPortObject.getDBSession();

        final ExecutionContext exec = parameters.getExecutionContext();
        final ConnectableCsvLoaderNodeSettings customSettings = parameters.getCustomSettings();
        final DBTable table = parameters.getCustomSettings().getTableNameModel().toDBTable();
        m_typeMappingService = DBTypeMappingRegistry.getInstance().getDBTypeMappingService(session.getDBType());
        DataTypeMappingConfiguration<SQLType> externalToKnimeTypeMapping = m_sessionPortObject
            .getExternalToKnimeTypeMapping().resolve(m_typeMappingService, DataTypeMappingDirection.EXTERNAL_TO_KNIME);

        final List<DBColumn> normalColumns = new ArrayList<>();
        final List<DBColumn> partitionColumns = new ArrayList<>();

        fillColumnLists(exec, table, normalColumns, partitionColumns, session);

        validateColumns(parameters.getRowInput().getDataTableSpec(),
            m_sessionPortObject.getKnimeToExternalTypeMapping(), session, normalColumns, partitionColumns);

        // Create and write to the temporary file
        final Path temporaryFile = createTempFile("knime2db", ".orc").toPath();
        delete(temporaryFile);
        try (AutoCloseable temporaryFileDeleter = () -> delete(temporaryFile)) {
            exec.checkCanceled();
            exec.setMessage("Writing temporary file...");
            writeORC(parameters.getRowInput(), temporaryFile, normalColumns, partitionColumns);
            exec.setProgress(0.25, "Temporary file written. Uploading file ...");
            exec.checkCanceled();
            // Upload the file
            final ConnectionMonitor<?> connectionMonitor = new ConnectionMonitor<>();
            try (AutoCloseable connectionMonitorCloser = () -> connectionMonitor.closeAll()) {
                String targetFolder = customSettings.getTargetFolderModel().getStringValue();
                if (!targetFolder.endsWith("/")) {
                    targetFolder += '/';
                }
                final ConnectionInformation connectionInformation =
                    parameters.getConnectionInformationPortObject().getConnectionInformation();
                final RemoteFile<?> targetDirectory =
                    createRemoteFile(new URI(connectionInformation.toURI() + encodePath(targetFolder)),
                        connectionInformation, connectionMonitor);
                LOGGER.debugWithFormat("Target folder URI: \"%s\"", targetDirectory.getURI());
                final RemoteFile<?> targetFile = resolveFileName(targetDirectory, temporaryFile);
                try (AutoCloseable targetFileDeleter = () -> {
                    if (targetFile.exists() && !targetFile.delete()) {
                        customSettings.getModelDelegate().setWarning("The target file could not be deleted.");
                    }
                }) {
                    LOGGER.debugWithFormat("Target file URI: \"%s\"", targetFile.getURI());
                    copyLocalFile(temporaryFile, targetFile, exec);
                    exec.setProgress(0.5, "File uploaded.");
                    LOGGER.debug("The content has been copied to the target file.");

                    // Load the data
                    session.getAgent(DBLoader.class).load(exec,
                        new DBLoadTableFromFileParameters<HiveLoaderParameters>(null, targetFile.getFullName(), table,
                            new HiveLoaderParameters(normalColumns, partitionColumns, targetFile)));
                }
            }
        }
        // Output
        return new DBDataPortObject(m_sessionPortObject, session.getAgent(DBMetadataReader.class)
            .getDBDataObject(exec, table.getSchemaName(), table.getName(), externalToKnimeTypeMapping));
    }

    /**
     * Method that uses the describe formatted table command to get the normal and partition column information.
     * @param exec {@link ExecutionMonitor}
     * @param table the hive table
     * @param normalColumns list to fill with the normal column names in the order they appear
     * @param partitionColumns list to fill with the partition column names in the order they appear
     * @param session the {@link DBSession} to use
     * @throws Exception if an exception happens
     */
    private static void fillColumnLists(final ExecutionMonitor exec, final DBTable table, final List<DBColumn> normalColumns,
        final List<DBColumn> partitionColumns, final DBSession session) throws Exception {
        String getCreateTable = "DESCRIBE FORMATTED " + session.getDialect().createFullName(table);
        try (java.sql.Connection connection = session.getConnectionProvider().getConnection(exec)) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet result = statement.executeQuery(getCreateTable)) {
                    String prevRow = "";
                    String row = "";
                    while (result.next()) {
                        prevRow = row;
                        row = result.getString(1);
                        if (row.startsWith("# col_name")) {
                            if (prevRow.startsWith("# Partition Information")) {
                                row = findColumnNames(partitionColumns, result);
                            } else {
                                row = findColumnNames(normalColumns, result);
                            }
                        }
                    }

                }
            }
        } catch (final Throwable throwable) {
            if (throwable instanceof Exception) {
                throw (Exception)throwable;
            } else {
                throw new SQLException(throwable.getMessage(), throwable);
            }
        }

    }

    private static String findColumnNames(final List<DBColumn> columns, final ResultSet result)
        throws SQLException {
        String row = "";
        while (result.next()) {
            row = result.getString(1);
            if (row.startsWith("#")) {
                break;
            } else {
                if (!row.isEmpty()) {
                    String type = result.getString(2);
                    DBColumn column = new org.knime.database.dialect.DBColumn(row, type, false);
                    columns.add(column);
                }
            }
        }
        return row;
    }

    private static void writeORC(final RowInput rowInput, final Path temporaryFile,
        final List<DBColumn> normalColumns, final List<DBColumn> partitionColumns) throws Exception {

        List<DBColumn> cols =
            Stream.concat(normalColumns.stream(), partitionColumns.stream()).collect(Collectors.toList());
        RemoteFile<Connection> file = RemoteFileFactory.createRemoteFile(temporaryFile.toUri(), null, null);
        final DataTableSpec spec = rowInput.getDataTableSpec();

        try (OrcKNIMEWriter writer = new OrcKNIMEWriter(file, spec, -1, "NONE", getORCTypesMapping(spec, cols))) {

            DataRow row;

            while ((row = rowInput.poll()) != null) {
                writer.writeRow(row);
            }
        } finally {
            rowInput.close();
            LOGGER.debug("Written file " + temporaryFile + " ");
        }
    }

    private static DataTypeMappingConfiguration<TypeDescription> getORCTypesMapping(final DataTableSpec spec,
        final List<DBColumn> cols) {

        List<TypeDescription> orcTypes = new ArrayList<>();
        for (DBColumn dbCol : cols) {
            String type = dbCol.getType();
            orcTypes.add(HiveTypeUtil.hivetoOrcType(type));
        }

        final DataTypeMappingConfiguration<TypeDescription> configuration =
            ORCTypeMappingService.getInstance().createMappingConfiguration(DataTypeMappingDirection.KNIME_TO_EXTERNAL);

        for (int i = 0; i < spec.getNumColumns(); i++) {
            DataColumnSpec knimeCol = spec.getColumnSpec(i);
            DataType dataType = knimeCol.getType();
            Collection<ConsumptionPath> consumPaths =
                ORCTypeMappingService.getInstance().getConsumptionPathsFor(dataType);
            boolean found = false;
            for (ConsumptionPath path : consumPaths) {

                if (path.getConsumerFactory().getDestinationType().equals(orcTypes.get(i))) {
                    found = true;

                    configuration.addRule(dataType, path);
                    break;
                }
            }
            if (!found) {
                String error = String.format("Could not find ConsumptionPath for %s to JDBC Type %s via ORC Type %s",
                    dataType, cols.get(i).getType(), orcTypes.get(i));
                LOGGER.error(error);
                throw new RuntimeException(error);
            }
        }

        return configuration;

    }
}
