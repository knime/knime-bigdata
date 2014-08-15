/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 *
 * History
 *   Created on 09.05.2014 by thor
 */
package com.knime.bigdata.hive.node.loader;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import org.knime.base.filehandling.NodeUtils;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObject;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.base.filehandling.remote.files.RemoteFileFactory;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.IntValue;
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
import org.knime.core.node.port.database.DatabaseConnectionPortObject;
import org.knime.core.node.port.database.DatabaseConnectionPortObjectSpec;
import org.knime.core.node.port.database.DatabaseConnectionSettings;
import org.knime.core.node.port.database.DatabasePortObject;
import org.knime.core.node.port.database.DatabasePortObjectSpec;
import org.knime.core.node.port.database.DatabaseQueryConnectionSettings;
import org.knime.core.node.port.database.DatabaseReaderConnection;
import org.knime.core.node.port.database.StatementManipulator;
import org.knime.core.util.FileUtil;

import com.knime.bigdata.hive.LicenseUtil;
import com.knime.bigdata.hive.utility.HiveUtility;

/**
 * Model for the Hive Loader node.
 *
 * @author Thorsten Meinl, KNIME.com, Zurich, Switzerland
 */
class HiveLoaderNodeModel extends NodeModel {
    private final HiveLoaderSettings m_settings = new HiveLoaderSettings();

    HiveLoaderNodeModel() {
        super(new PortType[]{ConnectionInformationPortObject.TYPE, BufferedDataTable.TYPE,
            DatabaseConnectionPortObject.TYPE}, new PortType[]{DatabasePortObject.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        LicenseUtil.instance.checkLicense();

        checkDatabaseSettings(inSpecs);
        checkUploadSettings(inSpecs);

        // We cannot provide a spec because it's not clear yet what the DB will return when the imported data
        // is read back into KNIME
        return new PortObjectSpec[]{null};
    }

    private void checkDatabaseSettings(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        DataTableSpec tableSpec = (DataTableSpec)inSpecs[1];

        // guess or apply type mapping
        m_settings.guessTypeMapping(tableSpec, false);

        // check table name
        if ((m_settings.tableName() == null) || m_settings.tableName().trim().isEmpty()) {
            throw new InvalidSettingsException("No table name given");
        }

        // throw exception if no data provided
        if (tableSpec.getNumColumns() == 0) {
            throw new InvalidSettingsException("No columns in input data.");
        }

        // check database connection
        DatabaseConnectionSettings connSettings =
            ((DatabaseConnectionPortObjectSpec)inSpecs[2]).getConnectionSettings(getCredentialsProvider());

        if ((connSettings.getJDBCUrl() == null) || connSettings.getJDBCUrl().isEmpty()
            || (connSettings.getDriver() == null) || connSettings.getDriver().isEmpty()) {
            throw new InvalidSettingsException("No valid database connection provided via second input port");
        }
        if (!(connSettings.getUtility() instanceof HiveUtility)) {
            throw new InvalidSettingsException("Only Hive database connections are supported");
        }

        for (String colName : m_settings.partitionColumns()) {
            DataColumnSpec cs = tableSpec.getColumnSpec(colName);
            if (cs == null) {
                throw new InvalidSettingsException("Partitioning column '" + colName
                    + "' does not exist in input table");
            } else if (cs.getType().isCompatible(DoubleValue.class) && !cs.getType().isCompatible(IntValue.class)) {
                throw new InvalidSettingsException("Double column '" + colName + "' cannot be used for partitioning");
            }
        }
    }

    private void checkUploadSettings(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        ConnectionInformationPortObjectSpec object = (ConnectionInformationPortObjectSpec)inSpecs[0];
        ConnectionInformation connInfo = object.getConnectionInformation();

        // Check if the port object has connection information
        if (connInfo == null) {
            throw new InvalidSettingsException("No connection information available");
        }

        if ((m_settings.targetFolder() == null) || m_settings.targetFolder().trim().isEmpty()) {
            throw new InvalidSettingsException("No target folder for data upload provided");
        }
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        exec.setProgress(null);
        ConnectionInformation connInfo = ((ConnectionInformationPortObject)inObjects[0]).getConnectionInformation();
        BufferedDataTable table = (BufferedDataTable)inObjects[1];
        DatabaseConnectionPortObject dbObj = (DatabaseConnectionPortObject)inObjects[2];
        File dataFile = writeDataToFile(table, exec.createSubProgress(0.1));
        ConnectionMonitor<?> connMonitor = new ConnectionMonitor<>();
        RemoteFile<? extends Connection> remoteFile = null;
        try {
            remoteFile = uploadFile(dataFile, connInfo, (ConnectionMonitor<? extends Connection>)connMonitor,
                exec.createSubExecutionContext(0.2));
            exec.checkCanceled();
            importData(remoteFile, table.getDataTableSpec(), dbObj.getConnectionSettings(getCredentialsProvider()),
                exec.createSubExecutionContext(0.5));
        } finally {
            Exception er = null;
            if (remoteFile != null) {
                try {
                    if (!remoteFile.delete()) {
                        setWarningMessage("Could not delete temporary import file on server");
                    }
                } catch (Exception e) {
                    er = e;
                }
            }
            connMonitor.closeAll();
            if (er != null) {
                throw er;
            }
        }

        // create output object
        exec.setProgress(0.8, "Determining table structure");
        DatabaseQueryConnectionSettings querySettings =
                new DatabaseQueryConnectionSettings(dbObj.getConnectionSettings(getCredentialsProvider()),
                    "SELECT * FROM " + m_settings.tableName());
        DatabaseReaderConnection conn = new DatabaseReaderConnection(querySettings);
        DataTableSpec tableSpec = conn.getDataTableSpec(getCredentialsProvider());
        DatabasePortObjectSpec outSpec = new DatabasePortObjectSpec(tableSpec, querySettings);
        DatabasePortObject retVal = new DatabasePortObject(outSpec);
        return new PortObject[]{retVal};
    }

    private File writeDataToFile(final BufferedDataTable table, final ExecutionMonitor execMon) throws IOException,
        CanceledExecutionException {
        File tempFile = FileUtil.createTempFile("hive-import", ".csv");

        double max = table.getRowCount();
        long count = 0;
        try (BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tempFile),"UTF-8"))) {
            for (DataRow row : table) {
                execMon.setProgress(count++ / max, "Writing table to temporary file (" + count + " rows)");
                execMon.checkCanceled();
                for (int i = 0; i < row.getNumCells(); i++) {
                    DataCell c = row.getCell(i);
                    if (c.isMissing()) {
                        out.write("\\N");
                    } else {
                        String s = row.getCell(i).toString();
                        if (s.indexOf('\n') >= 0) {
                            throw new IOException("Line breaks in cell contents are not supported (row '"
                                + row.getKey() + "', column '" + table.getDataTableSpec().getColumnSpec(i).getName()
                                + "')");
                        }
                        s = s.replace("\t", "\\\t"); // replace column delimiter

                        out.write(s);
                    }
                    if (i < row.getNumCells() - 1) {
                        out.write('\t');
                    } else {
                        out.write('\n');
                    }
                }
            }
        } catch (IOException | CanceledExecutionException ex) {
            Files.deleteIfExists(tempFile.toPath());
            throw ex;
        }

        return tempFile;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private RemoteFile<? extends Connection> uploadFile(final File dataFile, final ConnectionInformation connInfo,
        final ConnectionMonitor<? extends Connection> connMonitor, final ExecutionContext exec) throws Exception {
        exec.setMessage("Uploading import file to server");
        String targetFolder = m_settings.targetFolder();
        if (!targetFolder.endsWith("/")) {
            targetFolder += "/";
        }
        URI folderUri = new URI(connInfo.toURI().toString() + NodeUtils.encodePath(targetFolder));
        RemoteFile folder = RemoteFileFactory.createRemoteFile(folderUri, connInfo, connMonitor);
        folder.mkDirs(true);

        RemoteFile sourceFile = RemoteFileFactory.createRemoteFile(dataFile.toURI(), null, null);

        URI targetUri = new URI(folder.getURI() + NodeUtils.encodePath(dataFile.getName()));
        RemoteFile target = RemoteFileFactory.createRemoteFile(targetUri, connInfo, connMonitor);
        target.write(sourceFile, exec);
        exec.setProgress(1);
        return target;
    }

    private void importData(final RemoteFile<? extends Connection> remoteFile, final DataTableSpec tableSpec,
        final DatabaseConnectionSettings connSettings, final ExecutionContext exec) throws Exception {
        Connection conn = connSettings.createConnection(getCredentialsProvider());

        // check if table already exists and whether we should drop it
        boolean tableAlreadyExists = false;
        try (ResultSet rs = conn.getMetaData().getTables(null, null, m_settings.tableName(), null)) {
            if (rs.next()) {
                if (m_settings.dropTableIfExists()) {
                    try (Statement st = conn.createStatement()) {
                        getLogger().debug("Dropping existing table '" + m_settings.tableName() + "'");
                        exec.setMessage("Dropping existing table '" + m_settings.tableName() + "'");
                        st.execute("DROP TABLE " + m_settings.tableName());
                    }
                } else {
                    tableAlreadyExists = true;
                }
            }
        }

        exec.setMessage("Importing data");
        List<String> normalColumns = new ArrayList<>();
        for (DataColumnSpec cs : tableSpec) {
            normalColumns.add(cs.getName());
        }
        StatementManipulator manip = connSettings.getUtility().getStatementManipulator();

        try (Statement st = conn.createStatement()) {
            if (!m_settings.partitionColumns().isEmpty()) {
                importPartitionedData(remoteFile, tableSpec, normalColumns, manip, tableAlreadyExists, st, exec);
            } else {
                if (!tableAlreadyExists) {
                    exec.setProgress(0, "Creating table");
                    String createTableCmd =
                        buildCreateTableCommand(m_settings.tableName(), tableSpec, normalColumns,
                            new ArrayList<String>(), manip);
                    getLogger().debug("Executing '" + createTableCmd + "'");
                    st.execute(createTableCmd);
                }

                exec.setProgress(0.5, "Loading data into table");
                String buildTableCmd = buildLoadCommand(remoteFile, m_settings.tableName());
                getLogger().debug("Executing '" + buildTableCmd + "'");
                st.execute(buildTableCmd);
            }
        }
        exec.setProgress(1);
    }

    private void importPartitionedData(final RemoteFile<? extends Connection> remoteFile, final DataTableSpec tableSpec,
        final List<String> normalColumns, final StatementManipulator manip, final boolean tableAlreadyExists,
        final Statement st, final ExecutionContext exec) throws Exception {
        String tempTableName = m_settings.tableName() + "_" + Long.toHexString(Math.abs(new Random().nextLong()));

        // first create an unpartitioned table
        exec.setProgress(0, "Creating temporary table");
        String createTableCmd =
            buildCreateTableCommand(tempTableName, tableSpec, normalColumns, new ArrayList<String>(), manip);
        getLogger().debug("Executing '" + createTableCmd + "'");
        st.execute(createTableCmd);

        for (String partCol : m_settings.partitionColumns()) {
            normalColumns.remove(partCol);
        }
        try {
            exec.setProgress(0.2, "Importing data from uploaded file");
            String loadTableCmd = buildLoadCommand(remoteFile, tempTableName);
            getLogger().debug("Executing '" + loadTableCmd + "'");
            st.execute(loadTableCmd);

            if (!tableAlreadyExists) {
                // now create a partitioned table and copy data from
                exec.setProgress(0.4, "Creating final table");
                createTableCmd =
                    buildCreateTableCommand(m_settings.tableName(), tableSpec, normalColumns,
                        m_settings.partitionColumns(), manip);
                getLogger().debug("Executing '" + createTableCmd + "'");
                st.execute(createTableCmd);
            }

            exec.setProgress(0.6, "Copying data to partitioned table");
            st.execute("SET hive.exec.dynamic.partition = true");
            st.execute("SET hive.exec.dynamic.partition.mode = nonstrict");
            String insertCmd =
                buildInsertCommand(remoteFile, tempTableName, m_settings.tableName(), normalColumns,
                    m_settings.partitionColumns(), manip);
            getLogger().debug("Executing '" + insertCmd + "'");
            st.execute(insertCmd);
        } finally {
            exec.setProgress(0.9, "Deleting temporary table");
            st.execute("DROP TABLE " + tempTableName);
        }
        exec.setProgress(1);
    }

    private String buildCreateTableCommand(final String tableName, final DataTableSpec tableSpec,
        final Collection<String> normalColumns, final Collection<String> partitionColumns,
        final StatementManipulator manip) {
        StringBuilder buf = new StringBuilder();
        buf.append("CREATE TABLE " + tableName + " (\n");

        for (String col : normalColumns) {
            buf.append("   ");
            buf.append(manip.quoteColumn(col));
            buf.append(" ");
            buf.append(m_settings.typeMapping(col));
            buf.append(",\n");
        }
        assert tableSpec.getNumColumns() > 0 : "No columns in input table";
        buf.deleteCharAt(buf.length() - 2); // delete the comma
        buf.append(")\n");

        if (!partitionColumns.isEmpty()) {
            buf.append("PARTITIONED BY (");
            for (String partCol : m_settings.partitionColumns()) {
                buf.append(manip.quoteColumn(partCol));
                buf.append(" ");
                buf.append(m_settings.typeMapping(partCol));
                buf.append(",");
            }
            buf.deleteCharAt(buf.length() - 1);
            buf.append(")\n");
        }
        buf.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' ESCAPED BY '\\\\'\n");
        buf.append("STORED AS TEXTFILE");
        return buf.toString();
    }

    private String buildLoadCommand(final RemoteFile<? extends Connection> remoteFile, final String tableName)
            throws Exception {
        return "LOAD DATA LOCAL INPATH '" + remoteFile.getFullName() + "' INTO TABLE " + tableName;
    }

    private String buildInsertCommand(final RemoteFile<? extends Connection> remoteFile, final String sourceTableName,
        final String destTableName, final Collection<String> normalColumns, final Collection<String> partitionColumns,
        final StatementManipulator manip) {

        StringBuilder buf = new StringBuilder();
        buf.append("INSERT INTO TABLE ").append(destTableName);
        buf.append(" PARTITION (");
        for (String partCol : partitionColumns) {
            buf.append(manip.quoteColumn(partCol)).append(",");
        }
        buf.deleteCharAt(buf.length() - 1);
        buf.append(")\n");

        buf.append("SELECT ");
        for (String col : normalColumns) {
            buf.append(manip.quoteColumn(col)).append(",");
        }
        for (String col : partitionColumns) {
            buf.append(manip.quoteColumn(col)).append(",");
        }
        buf.deleteCharAt(buf.length() - 1);
        buf.append("\nFROM ").append(sourceTableName);

        return buf.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec) throws IOException,
        CanceledExecutionException {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec) throws IOException,
        CanceledExecutionException {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        HiveLoaderSettings hs = new HiveLoaderSettings();
        hs.loadSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reset() {

    }
}
