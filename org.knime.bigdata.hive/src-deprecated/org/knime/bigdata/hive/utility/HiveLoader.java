/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
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
package org.knime.bigdata.hive.utility;

import java.io.File;
import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.knime.base.filehandling.NodeUtils;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.base.filehandling.remote.files.RemoteFileFactory;
import org.knime.base.filehandling.remote.files.RemoteFileHandlerRegistry;
import org.knime.bigdata.hdfs.filehandler.HDFSCompatibleConnectionInformation;
import org.knime.bigdata.hdfs.filehandler.HDFSRemoteFileHandler;
import org.knime.cloud.core.file.CloudRemoteFile;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.database.DatabaseConnectionSettings;
import org.knime.core.node.port.database.DatabaseUtility;
import org.knime.core.node.port.database.StatementManipulator;
import org.knime.core.node.workflow.CredentialsProvider;

/** @author Tobias Koetter, KNIME.com */
@Deprecated
public class HiveLoader {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(HiveLoader.class);

    private boolean m_isImpala = false;

    /**
     * Uploads the local file <code>dataFile</code> to a remote target defined in <code>settings
     * </code>
     *
     * @param dataFile the {@link File} to upload
     * @param connInfo the {@link ConnectionInformation}
     * @param connMonitor the {@link ConnectionMonitor}
     * @param exec the {@link ExecutionContext}
     * @param settings the {@link LoaderSettings}
     * @return the {@link RemoteFile}
     * @throws Exception if the file could not be uploaded to the remote file system
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public RemoteFile<? extends Connection> uploadFile(
        final File dataFile,
        final ConnectionInformation connInfo,
        final ConnectionMonitor<? extends Connection> connMonitor,
        final ExecutionContext exec,
        final LoaderSettings settings)
                throws Exception {
        LOGGER.debug("Uploading local file " + dataFile.getPath());
        exec.setMessage("Uploading import file to server");
        String targetFolder = settings.targetFolder();
        if (!targetFolder.endsWith("/")) {
            targetFolder += "/";
        }
        final URI folderUri = new URI(connInfo.toURI().toString() + NodeUtils.encodePath(targetFolder));
        LOGGER.debug("Create remote folder with URI " + folderUri);
        final RemoteFile remoteFolder =
                RemoteFileFactory.createRemoteFile(folderUri, connInfo, connMonitor);
        remoteFolder.mkDirs(true);
        LOGGER.debug("Remote folder created");
        final RemoteFile sourceFile = RemoteFileFactory.createRemoteFile(dataFile.toURI(), null, null);

        final URI targetUri = new URI(remoteFolder.getURI() + NodeUtils.encodePath(dataFile.getName()));
        LOGGER.debug("Create remote file with URI " + targetUri);
        final RemoteFile target = RemoteFileFactory.createRemoteFile(targetUri, connInfo, connMonitor);
        LOGGER.debug("Remote file created. Start writing file content...");
        target.write(sourceFile, exec);
        LOGGER.debug("File content sucessful written to remote file");
        exec.setProgress(1);
        return target;
    }

    /**
     * Imports the data contained in the <code>remoteFile</code> into the table defined in <code>
     *  settings </code>
     *
     * @param remoteFile the {@link RemoteFile} that contains the Hive table data
     * @param columnNames column names
     * @param connSettings the {@link DatabaseConnectionSettings} to connect to Hive
     * @param exec {@link ExecutionContext}
     * @param settings {@link LoaderSettings}
     * @param cp {@link CredentialsProvider}
     * @throws Exception if the table could not be created in Hive
     */
    public void importData(
        final RemoteFile<? extends Connection> remoteFile,
        final List<String> columnNames,
        final DatabaseConnectionSettings connSettings,
        final ExecutionContext exec,
        final LoaderSettings settings,
        final CredentialsProvider cp)
                throws Exception {

        assert (columnNames != null) && !columnNames.isEmpty() : "No columns in input table";
        final DatabaseUtility utility = connSettings.getUtility();
        connSettings.execute(cp, conn -> {
            // check if table already exists and whether we should drop it
            boolean tableAlreadyExists = false;
            final String tableName = settings.tableName();
            final boolean dropTableIfExists = settings.dropTableIfExists();
            LOGGER.debug("Column names: " + columnNames);
            final Collection<String> partitionColumns = settings.partitionColumns();
            LOGGER.debug("Partition columns: " + partitionColumns);

            if (utility.tableExists(conn, tableName)) {
                LOGGER.debug("The table " + tableName + " already exists");
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
            LOGGER.debug("Importing data");
            final StatementManipulator manip = utility.getStatementManipulator();
            try (Statement st = conn.createStatement()) {
                if (!partitionColumns.isEmpty()) {
                    importPartitionedData(
                        remoteFile, columnNames, manip, tableAlreadyExists, st, exec, settings);
                } else {
                    if (!tableAlreadyExists) {
                        //if table does not exists, we can use LOAD DATA
                        exec.setProgress(0, "Creating table");
                        final String createTableCmd =
                                buildCreateTableCommand(
                                    tableName, columnNames, new ArrayList<String>(), manip, settings);
                        LOGGER.debug("Executing '" + createTableCmd + "'");
                        st.execute(createTableCmd);
                        LOGGER.debug("Table sucessful created");
                        exec.setProgress(0.5, "Loading data into table");
                        final String buildTableCmd = buildLoadCommand(remoteFile, tableName);
                        LOGGER.info("Executing '" + buildTableCmd + "'");
                        st.execute(buildTableCmd);
                        LOGGER.debug("Data loaded sucessfully");
                    } else{
                        //Appending to an existing table using a temp table
                        LOGGER.debug("Start loading data...");
                        final String tempTableName = createTempTable(columnNames, manip, st, exec, settings);
                        loadToTempTable(remoteFile, st, exec, tempTableName);
                        try{
                            LOGGER.debug("Copying data to existing table");
                            exec.setProgress(0.6, "Copying data to existing table");
                            final String insertCmd =
                                    buildInsertCommand(
                                        tempTableName,
                                        settings.tableName(),
                                        columnNames,
                                        null,
                                        manip);
                            LOGGER.debug("Executing '" + insertCmd + "'");
                            st.execute(insertCmd);
                        } finally {
                            deleteTempTable(st, exec, tempTableName);
                        }
                        exec.setProgress(1);
                    }
                }
            }
            return null;
        });
        exec.setProgress(1);
    }

    private void importPartitionedData(
        final RemoteFile<? extends Connection> remoteFile,
        final List<String> columnNames,
        final StatementManipulator manip,
        final boolean tableAlreadyExists,
        final Statement st,
        final ExecutionContext exec,
        final LoaderSettings settings)
                throws Exception {
        LOGGER.debug("Start load partitioned data...");

        final String tempTableName = createTempTable(columnNames, manip, st, exec, settings);
        final List<String> normalColumns = new ArrayList<>(columnNames);
        for (final String partCol : settings.partitionColumns()) {
            normalColumns.remove(partCol);
        }
        try {
            loadToTempTable(remoteFile, st, exec, tempTableName);

            if (!tableAlreadyExists) {
                // now create a partitioned table and copy data from
                exec.setProgress(0.4, "Creating final table");
                String createTableCmd =
                        buildCreateTableCommand(
                            settings.tableName(), normalColumns, settings.partitionColumns(), manip, settings);
                LOGGER.debug("Executing '" + createTableCmd + "'");
                st.execute(createTableCmd);
            }
            LOGGER.debug("Copying data to partitioned table");
            exec.setProgress(0.6, "Copying data to partitioned table");

            if (!isImpala()) {
                final String setDynamicPartStmt = "SET hive.exec.dynamic.partition=true";
                final String nonStrictStmt = "SET hive.exec.dynamic.partition.mode=nonstrict";
                LOGGER.debug("Executing '" + setDynamicPartStmt + "'");
                st.execute(setDynamicPartStmt);
                LOGGER.debug("Executing  " + nonStrictStmt + "'");
                st.execute(nonStrictStmt);
            }

            final String insertCmd =
                    buildInsertCommand(
                        tempTableName,
                        settings.tableName(),
                        normalColumns,
                        settings.partitionColumns(),
                        manip);
            LOGGER.debug("Executing '" + insertCmd + "'");
            st.execute(insertCmd);

        } finally {
            deleteTempTable(st, exec, tempTableName);
        }
        exec.setProgress(1);
    }


    private String createTempTable(
        final List<String> columnNames,
        final StatementManipulator manip,
        final Statement st,
        final ExecutionContext exec,
        final LoaderSettings settings)
                throws SQLException {
        final String tempTableName =
                settings.tableName() + "_" + UUID.randomUUID().toString().replace('-', '_');
        LOGGER.debug("Creating temporary table " + tempTableName);
        // first create an unpartitioned table
        exec.setProgress(0, "Creating temporary table");
        String createTableCmd =
                buildCreateTableCommand(
                    tempTableName, columnNames, Collections.<String>emptyList(), manip, settings);
        LOGGER.debug("Executing '" + createTableCmd + "'");
        st.execute(createTableCmd);
        LOGGER.debug("Temporary table sucessful created");
        return tempTableName;
    }

    private void loadToTempTable(
        final RemoteFile<? extends Connection> remoteFile,
        final Statement st,
        final ExecutionContext exec,
        final String tempTableName)
                throws Exception, SQLException {
        exec.setProgress(0.2, "Importing data from uploaded file");
        final String loadTableCmd = buildLoadCommand(remoteFile, tempTableName);
        LOGGER.debug("Executing '" + loadTableCmd + "'");
        st.execute(loadTableCmd);
    }

    private void deleteTempTable(
        final Statement st, final ExecutionContext exec, final String tempTableName)
                throws SQLException {
        exec.setProgress(0.9, "Deleting temporary table");
        final String dropTable = "DROP TABLE " + tempTableName;
        LOGGER.info("Executing '" + dropTable + "'");
        st.execute(dropTable);
    }


    private String buildCreateTableCommand(
        final String tableName,
        final Collection<String> columnNames,
        final Collection<String> partitionColumns,
        final StatementManipulator manip,
        final LoaderSettings settings) {
        final StringBuilder buf = new StringBuilder();
        buf.append("CREATE TABLE " + tableName + " (\n");

        for (final String col : columnNames) {
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
            for (final String partCol : settings.partitionColumns()) {
                buf.append(manip.quoteIdentifier(partCol));
                buf.append(" ");
                buf.append(settings.typeMapping(partCol));
                buf.append(",");
            }
            buf.deleteCharAt(buf.length() - 1);
            buf.append(")\n");
        }
        buf.append(
            "ROW FORMAT DELIMITED FIELDS TERMINATED BY '"
                    + settings.valueDelimiter()
                    + "' ESCAPED BY '\\\\'\n");
        buf.append("STORED AS TEXTFILE");

        // Starting with CDH 7, Impala blocks LOAD INTO on transactional tables.
        // HDP 3.1 requires by default transactional tables and allows LOAD INTO.
        // That's why we disable transactions in Impala, but not in Hive.
        if (isImpala()) {
            buf.append(" TBLPROPERTIES (\"transactional\"=\"false\")");
        }

        return buf.toString();
    }

    private static String buildLoadCommand(
        final RemoteFile<? extends Connection> remoteFile, final String tableName) throws Exception {

        final ConnectionInformation connInfo = remoteFile.getConnectionInformation();

        if (remoteFile instanceof CloudRemoteFile) {
            LOGGER.debug("Load data from cloud file system");
            final CloudRemoteFile<?> cloudRemoteFile = (CloudRemoteFile<?>) remoteFile;
            final String clusterInputPath = cloudRemoteFile.getHadoopFilesystemURI().toString();
            // Hive handles load via move, use the full URI for cloud file systems e.g S3 and Azure BlobStore
            return "LOAD DATA INPATH '" + clusterInputPath + "' INTO TABLE " + tableName;

        } else if (HDFSRemoteFileHandler.isSupportedConnection(connInfo)
                || connInfo instanceof HDFSCompatibleConnectionInformation) {
            LOGGER.debug("Load data from hdfs");
            // Hive handles load via move, use Hive default FS URI and provide only input file path
            return "LOAD DATA INPATH '" + remoteFile.getFullName() + "' INTO TABLE " + tableName;

        } else if (RemoteFileHandlerRegistry.getProtocol(connInfo.getProtocol()).getName().equalsIgnoreCase("dbfs")) {
            LOGGER.debug("Load data from dbfs");
            // DBFS is the default FS on Databricks, use the input file path only
            return "LOAD DATA INPATH '" + remoteFile.getFullName() + "' INTO TABLE " + tableName;

        } else {
            LOGGER.debug("Load data from local file system");
            return "LOAD DATA LOCAL INPATH '" + remoteFile.getFullName() + "' INTO TABLE " + tableName;
        }
    }

    private static String buildInsertCommand(
        final String sourceTableName,
        final String destTableName,
        final Collection<String> normalColumns,
        final Collection<String> partitionColumns,
        final StatementManipulator manip) {

        final StringBuilder buf = new StringBuilder();
        buf.append("INSERT INTO TABLE ").append(destTableName);
        if(partitionColumns != null){
            buf.append(" PARTITION (");
            for (final String partCol : partitionColumns) {
                buf.append(manip.quoteIdentifier(partCol)).append(",");
            }
            buf.deleteCharAt(buf.length() - 1);
            buf.append(")");
        }
        buf.append("\n");
        buf.append("SELECT ");
        for (final String col : normalColumns) {
            buf.append(manip.quoteIdentifier(col)).append(",");
        }
        if(partitionColumns != null){
            for (final String col : partitionColumns) {
                buf.append(manip.quoteIdentifier(col)).append(",");
            }
        }
        buf.deleteCharAt(buf.length() - 1);
        buf.append("\nFROM ").append(sourceTableName);

        return buf.toString();
    }


    /**
     * Returns boolean indicating whether the loader is used in an Impala context
     * @return the isImpala
     * */
    public boolean isImpala() {
        return m_isImpala;
    }

    /** Sets the Loader to ImpalalMode */
    public void setImpala() {
        m_isImpala = true;
    }
}
