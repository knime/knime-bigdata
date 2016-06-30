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
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.Date;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObject;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.IntValue;
import org.knime.core.data.date.DateAndTimeValue;
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
import org.knime.core.node.port.database.DatabaseConnectionPortObject;
import org.knime.core.node.port.database.DatabaseConnectionPortObjectSpec;
import org.knime.core.node.port.database.DatabaseConnectionSettings;
import org.knime.core.node.port.database.DatabasePortObject;
import org.knime.core.node.port.database.DatabasePortObjectSpec;
import org.knime.core.node.port.database.DatabaseQueryConnectionSettings;
import org.knime.core.node.port.database.DatabaseReaderConnection;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.util.FileUtil;

import com.knime.bigdata.hdfs.filehandler.HDFSRemoteFileHandler;
import com.knime.bigdata.hive.utility.HiveLoader;
import com.knime.bigdata.hive.utility.HiveLoaderSettings;
import com.knime.bigdata.hive.utility.HiveUtility;

/**
 * Model for the Hive Loader node.
 *
 * @author Thorsten Meinl, KNIME.com, Zurich, Switzerland
 */
class HiveLoaderNodeModel extends NodeModel {
    private static final NodeLogger LOGGER = NodeLogger.getLogger(HiveLoaderNodeModel.class);
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
        HiveUtility.LICENSE_CHECKER.checkLicenseInNode();

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

        // check that at least one non partitioned column exists
        if (tableSpec.getNumColumns() == m_settings.partitionColumns().size()) {
            throw new InvalidSettingsException("Cannot use all columns for partitioning.");
        }
    }

    private void checkUploadSettings(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        ConnectionInformationPortObjectSpec connSpec = (ConnectionInformationPortObjectSpec) inSpecs[0];
        ConnectionInformation connInfo = connSpec.getConnectionInformation();
        final CredentialsProvider cp = getCredentialsProvider();
        DatabaseConnectionPortObjectSpec dbSpec = (DatabaseConnectionPortObjectSpec) inSpecs[2];
        DatabaseConnectionSettings dbSettings = dbSpec.getConnectionSettings(cp);

        // Check if the port object has connection information
        if (connInfo == null) {
            throw new InvalidSettingsException("No connection information available");
        }

        if ((m_settings.targetFolder() == null) || m_settings.targetFolder().trim().isEmpty()) {
            throw new InvalidSettingsException("No target folder for data upload provided");
        }

        if (HDFSRemoteFileHandler.isSupportedConnection(connInfo)
                && !connInfo.useKerberos()
                && connInfo.getUser() != null && !connInfo.getUser().isEmpty()
                && dbSettings.getUserName(cp) != null && !dbSettings.getUserName(cp).isEmpty()
                && !connInfo.getUser().equals(dbSettings.getUserName(cp))) {

            setWarningMessage("Different HDFS and Hive user found, this might result in permission problems.");
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
        final CredentialsProvider cp = getCredentialsProvider();
        DatabaseConnectionSettings dbConn = dbObj.getConnectionSettings(cp);
        File dataFile = writeDataToFile(table, exec.createSubProgress(0.1), dbConn.getTimeZone());
        ConnectionMonitor<?> connMonitor = new ConnectionMonitor<>();
        RemoteFile<? extends Connection> remoteFile = null;
        try {
            final HiveLoader hiveLoader = HiveLoader.getInstance();
            remoteFile = hiveLoader.uploadFile(dataFile, connInfo, (ConnectionMonitor<? extends Connection>)connMonitor,
                exec.createSubExecutionContext(0.2), m_settings);
            exec.checkCanceled();
            final DataTableSpec tableSpec = table.getDataTableSpec();
            final List<String> normalColumns = new ArrayList<>();
            for (final DataColumnSpec cs : tableSpec) {
                normalColumns.add(cs.getName());
            }
            hiveLoader.importData(remoteFile, normalColumns, dbConn,
                exec.createSubExecutionContext(0.5), m_settings, cp);
        } finally {
            Exception er = null;
            if (remoteFile != null) {
                try {
                    if (remoteFile.exists() && !remoteFile.delete()) {
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
                new DatabaseQueryConnectionSettings(dbConn, "SELECT * FROM " + m_settings.tableName());
        DatabaseReaderConnection conn = new DatabaseReaderConnection(querySettings);
        DataTableSpec tableSpec = conn.getDataTableSpec(cp);
        DatabasePortObjectSpec outSpec = new DatabasePortObjectSpec(tableSpec, querySettings);
        DatabasePortObject retVal = new DatabasePortObject(outSpec);
        return new PortObject[]{retVal};
    }

    private File writeDataToFile(final BufferedDataTable table, final ExecutionMonitor execMon, final TimeZone timeZone)
            throws IOException, CanceledExecutionException {
        final File tempFile = FileUtil.createTempFile("hive-import", ".csv");
        LOGGER.debug("Start writing KNIME table to temporary file " + tempFile.getPath());
        final double max = table.size();
        LOGGER.debug("Table structure " + table.getSpec());
        LOGGER.debug("No of rows to write " + max);
        long count = 0;
        final SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyy-MM-dd HH:mm:ss.SSS");
        final SimpleDateFormat dateFormat = new SimpleDateFormat("yyy-MM-dd");
        try (BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tempFile),"UTF-8"))) {
            for (DataRow row : table) {
                execMon.setProgress(count++ / max, "Writing table to temporary file (" + count + " rows)");
                execMon.checkCanceled();
                for (int i = 0; i < row.getNumCells(); i++) {
                    DataCell c = row.getCell(i);
                    if (c.isMissing()) {
                        out.write("\\N");
                    } else {
                        final String s;
                        if (c instanceof DateAndTimeValue) {
                            final DateAndTimeValue d = (DateAndTimeValue)c;
                            final long corrDate = d.getUTCTimeInMillis() - timeZone.getOffset(d.getUTCTimeInMillis());
                            final Date date = new Date(corrDate);
                            final Format format;
                            if (d.hasTime()) {
                                format = dateTimeFormat;
                            } else {
                                format = dateFormat;
                            }
                            s = format.format(date);
                        } else {
                            s = c.toString();
                        }
                        if (s.indexOf('\n') >= 0) {
                            throw new IOException("Line breaks in cell contents are not supported (row '"
                                + row.getKey() + "', column '" + table.getDataTableSpec().getColumnSpec(i).getName()
                                + "')");
                        }
                        final String sr = s.replace("\t", "\\\t"); // replace column delimiter
                        out.write(sr);
                    }
                    if (i < row.getNumCells() - 1) {
                        out.write('\t');
                    } else {
                        out.write('\n');
                    }
                }
            }
        } catch (IOException | CanceledExecutionException ex) {
            LOGGER.debug("Exception while writing to temporary file: " + ex.getMessage());
            Files.deleteIfExists(tempFile.toPath());
            throw ex;
        }
        LOGGER.debug("Temporary file successful created at " + tempFile.getPath());
        return tempFile;
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
