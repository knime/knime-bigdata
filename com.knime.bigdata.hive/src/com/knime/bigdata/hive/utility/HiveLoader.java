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
 *   Created on 14.04.2015 by koetter
 */
package com.knime.bigdata.hive.utility;

import java.io.File;
import java.net.URI;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.knime.base.filehandling.NodeUtils;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.base.filehandling.remote.files.RemoteFileFactory;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.database.DatabaseConnectionSettings;
import org.knime.core.node.port.database.DatabaseUtility;
import org.knime.core.node.port.database.StatementManipulator;
import org.knime.core.node.workflow.CredentialsProvider;

import com.knime.licenses.LicenseException;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public final class HiveLoader {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(HiveLoader.class);

    private static volatile HiveLoader instance;

    private HiveLoader() throws LicenseException {
        HiveUtility.LICENSE_CHECKER.checkLicense();
    }

    /**
     * Returns the only instance of this class.
     * @return the only instance
     * @throws LicenseException if the user does not have the necessary licence
     */
    public static HiveLoader getInstance() throws LicenseException {
        if (instance == null) {
            synchronized (HiveLoader.class) {
                if (instance == null) {
                    instance = new HiveLoader();
                }
            }
        }
        return instance;
    }

    /**
     * @param dataFile the {@link File} to upload
     * @param connInfo the {@link ConnectionInformation}
     * @param connMonitor the {@link ConnectionMonitor}
     * @param exec the {@link ExecutionContext}
     * @param settings the {@link HiveLoaderSettings}
     * @return the {@link RemoteFile}
     * @throws Exception if the file could not be uploaded to the remote file system
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public RemoteFile<? extends Connection> uploadFile(final File dataFile, final ConnectionInformation connInfo,
        final ConnectionMonitor<? extends Connection> connMonitor, final ExecutionContext exec,
        final HiveLoaderSettings settings) throws Exception {
        LOGGER.debug("Uploading local file " + dataFile.getPath());
        exec.setMessage("Uploading import file to server");
        String targetFolder = settings.targetFolder();
        if (!targetFolder.endsWith("/")) {
            targetFolder += "/";
        }
        final URI folderUri = new URI(connInfo.toURI().toString() + NodeUtils.encodePath(targetFolder));
        LOGGER.debug("Create remote folder with URI " + folderUri);
        final RemoteFile remoteFolder = RemoteFileFactory.createRemoteFile(folderUri, connInfo, connMonitor);
        remoteFolder.mkDirs(true);
        LOGGER.debug("Remote folder created");
        RemoteFile sourceFile = RemoteFileFactory.createRemoteFile(dataFile.toURI(), null, null);

        URI targetUri = new URI(remoteFolder.getURI() + NodeUtils.encodePath(dataFile.getName()));
        LOGGER.debug("Create remote file with URI " + targetUri);
        RemoteFile target = RemoteFileFactory.createRemoteFile(targetUri, connInfo, connMonitor);
        LOGGER.debug("Remote file created. Start writing file content...");
        target.write(sourceFile, exec);
        LOGGER.debug("File content sucessful written to remote file");
        exec.setProgress(1);
        return target;
    }

    /**
     * @param remoteFile the {@link RemoteFile} that contains the Hive table data
     * @param columnNames column names
     * @param connSettings the {@link DatabaseConnectionSettings} to connect to Hive
     * @param exec {@link ExecutionContext}
     * @param settings {@link HiveLoaderSettings}
     * @param cp {@link CredentialsProvider}
     * @throws Exception if the table could not be created in Hive
     */
    public void importData(final RemoteFile<? extends Connection> remoteFile, final List<String> columnNames,
        final DatabaseConnectionSettings connSettings, final ExecutionContext exec,
        final HiveLoaderSettings settings, final CredentialsProvider cp) throws Exception {
        assert columnNames != null && !columnNames.isEmpty() : "No columns in input table";
        final DatabaseUtility utility = connSettings.getUtility();
        @SuppressWarnings("resource")
        final Connection conn = connSettings.createConnection(cp);
        // check if table already exists and whether we should drop it
        boolean tableAlreadyExists = false;
        final String tableName = settings.tableName();
        final boolean dropTableIfExists = settings.dropTableIfExists();
        LOGGER.debug("Column names: " + columnNames);
        final Collection<String> partitionColumns = settings.partitionColumns();
        LOGGER.debug("Partition columns: " + partitionColumns);
        synchronized (connSettings.syncConnection(conn)) {
            if (utility.tableExists(conn, tableName)) {
                LOGGER.debug("Hive table " + tableName + " already exists");
                if (dropTableIfExists) {
                    try (Statement st = conn.createStatement()) {
                        LOGGER.debug("Dropping existing table '" + tableName + "'");
                        exec.setMessage("Dropping existing table '" + tableName + "'");
                        st.execute("DROP TABLE " + tableName);
                        LOGGER.debug("Existing table droped");
                    }
                } else {
                    tableAlreadyExists = true;
                }
            }
            exec.setMessage("Importing data");
    //        List<String> normalColumns = new ArrayList<>(columnNames);
    //        for (DataColumnSpec cs : tableSpec) {
    //            normalColumns.add(cs.getName());
    //        }
            LOGGER.debug("Importing data");
            final StatementManipulator manip = utility.getStatementManipulator();
            try (Statement st = conn.createStatement()) {
                if (!partitionColumns.isEmpty()) {
                    importPartitionedData(remoteFile, columnNames, manip, tableAlreadyExists, st, exec,
                        settings);
                } else {
                    if (!tableAlreadyExists) {
                        exec.setProgress(0, "Creating table");
                        String createTableCmd = buildCreateTableCommand(tableName, columnNames,
                            new ArrayList<String>(), manip, settings);
                        LOGGER.debug("Executing '" + createTableCmd + "'");
                        st.execute(createTableCmd);
                        LOGGER.debug("Table sucessful created");
                    }
                    exec.setProgress(0.5, "Loading data into table");
                    String buildTableCmd = buildLoadCommand(remoteFile, tableName);
                    LOGGER.info("Executing '" + buildTableCmd + "'");
                    st.execute(buildTableCmd);
                    LOGGER.debug("Data loaded sucessfully");
                }
            }
        }
        exec.setProgress(1);
    }

    private static void importPartitionedData(final RemoteFile<? extends Connection> remoteFile,
        final List<String> columnNames, final StatementManipulator manip, final boolean tableAlreadyExists,
        final Statement st, final ExecutionContext exec, final HiveLoaderSettings settings) throws Exception {
        LOGGER.debug("Start load partitioned data...");
        String tempTableName = settings.tableName() + "_" + Long.toHexString(Math.abs(new Random().nextLong()));
        LOGGER.debug("Creating temporary table " + tempTableName);
        // first create an unpartitioned table
        exec.setProgress(0, "Creating temporary table");
        String createTableCmd =
            buildCreateTableCommand(tempTableName, columnNames, Collections.<String> emptyList(), manip, settings);
        LOGGER.debug("Executing '" + createTableCmd + "'");
        st.execute(createTableCmd);
        LOGGER.debug("Temporary table sucessful created");
        final List<String> normalColumns = new ArrayList<>(columnNames);
        for (String partCol : settings.partitionColumns()) {
            normalColumns.remove(partCol);
        }
        try {
            exec.setProgress(0.2, "Importing data from uploaded file");
            String loadTableCmd = buildLoadCommand(remoteFile, tempTableName);
            LOGGER.debug("Executing '" + loadTableCmd + "'");
            st.execute(loadTableCmd);

            if (!tableAlreadyExists) {
                // now create a partitioned table and copy data from
                exec.setProgress(0.4, "Creating final table");
                createTableCmd = buildCreateTableCommand(settings.tableName(), normalColumns,
                        settings.partitionColumns(), manip, settings);
                LOGGER.debug("Executing '" + createTableCmd + "'");
                st.execute(createTableCmd);
            }
            LOGGER.debug("Copying data to partitioned table");
            exec.setProgress(0.6, "Copying data to partitioned table");
            String setDynamicPartStmt = "SET hive.exec.dynamic.partition = true";
            String nonStrictStmt = "SET hive.exec.dynamic.partition.mode = nonstrict";
            LOGGER.debug("Executing '" + setDynamicPartStmt + "'");
            st.execute(setDynamicPartStmt);
            LOGGER.debug("Executing '" + nonStrictStmt + "'");
            st.execute(nonStrictStmt);
            String insertCmd =
                buildInsertCommand(remoteFile, tempTableName, settings.tableName(), normalColumns,
                    settings.partitionColumns(), manip);
            LOGGER.debug("Executing '" + insertCmd + "'");
            st.execute(insertCmd);
        } finally {
            exec.setProgress(0.9, "Deleting temporary table");
            final String dropTable = "DROP TABLE " + tempTableName;
            LOGGER.info("Executing '" + dropTable + "'");
            st.execute(dropTable);
        }
        exec.setProgress(1);
    }

    private static String buildCreateTableCommand(final String tableName, final Collection<String> columnNames,
        final Collection<String> partitionColumns, final StatementManipulator manip, final HiveLoaderSettings settings) {
        StringBuilder buf = new StringBuilder();
        buf.append("CREATE TABLE " + tableName + " (\n");

        for (String col : columnNames) {
            buf.append("   ");
            buf.append(manip.quoteIdentifier(col));
            buf.append(" ");
            buf.append(settings.typeMapping(col));
            buf.append(",\n");
        }
        buf.deleteCharAt(buf.length() - 2); // delete the comma
        buf.append(")\n");

        if (!partitionColumns.isEmpty()) {
            buf.append("PARTITIONED BY (");
            for (String partCol : settings.partitionColumns()) {
                buf.append(manip.quoteIdentifier(partCol));
                buf.append(" ");
                buf.append(settings.typeMapping(partCol));
                buf.append(",");
            }
            buf.deleteCharAt(buf.length() - 1);
            buf.append(")\n");
        }
        buf.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '" + settings.valueDelimiter() + "' ESCAPED BY '\\\\'\n");
        buf.append("STORED AS TEXTFILE");
        return buf.toString();
    }

    private static String buildLoadCommand(final RemoteFile<? extends Connection> remoteFile, final String tableName)
            throws Exception {
        return "LOAD DATA LOCAL INPATH '" + remoteFile.getFullName() + "' INTO TABLE " + tableName;
    }

    private static String buildInsertCommand(final RemoteFile<? extends Connection> remoteFile, final String sourceTableName,
        final String destTableName, final Collection<String> normalColumns, final Collection<String> partitionColumns,
        final StatementManipulator manip) {

        StringBuilder buf = new StringBuilder();
        buf.append("INSERT INTO TABLE ").append(destTableName);
        buf.append(" PARTITION (");
        for (String partCol : partitionColumns) {
            buf.append(manip.quoteIdentifier(partCol)).append(",");
        }
        buf.deleteCharAt(buf.length() - 1);
        buf.append(")\n");

        buf.append("SELECT ");
        for (String col : normalColumns) {
            buf.append(manip.quoteIdentifier(col)).append(",");
        }
        for (String col : partitionColumns) {
            buf.append(manip.quoteIdentifier(col)).append(",");
        }
        buf.deleteCharAt(buf.length() - 1);
        buf.append("\nFROM ").append(sourceTableName);

        return buf.toString();
    }

}
