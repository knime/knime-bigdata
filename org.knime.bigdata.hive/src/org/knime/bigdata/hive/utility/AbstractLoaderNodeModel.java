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
 *   Created on 18.01.2018 by "Mareike Höger, KNIME"
 */
package org.knime.bigdata.hive.utility;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.Date;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObject;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.bigdata.filehandling.local.HDFSLocalRemoteFileHandler;
import org.knime.bigdata.hdfs.filehandler.HDFSRemoteFileHandler;
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
import org.knime.core.node.port.database.DatabaseUtility;
import org.knime.core.node.port.database.reader.DBReader;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.util.FileUtil;

/**
 *
 * @author "Mareike Höger, KNIME"
 */
public abstract class AbstractLoaderNodeModel extends NodeModel {
    private static final NodeLogger LOGGER = NodeLogger.getLogger(AbstractLoaderNodeModel.class);
    private final LoaderSettings m_settings = new LoaderSettings();
    private final HiveLoader loader = new HiveLoader();


    /**
     * Constructor used by Hive- and ImpalaLoaderModel
     *
     * @param isImpala indicating if Loader is an Impala loader
     *
     */
    protected AbstractLoaderNodeModel(final boolean isImpala) {
        super(new PortType[]{ConnectionInformationPortObject.TYPE, BufferedDataTable.TYPE,
            DatabaseConnectionPortObject.TYPE}, new PortType[]{DatabasePortObject.TYPE});
        if(isImpala){
            this.loader.setImpala();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {

        checkDatabaseSettings(inSpecs, getCredentialsProvider());
        checkDatabaseConnection(inSpecs);
        checkUploadSettings(inSpecs);

        // We cannot provide a spec because it's not clear yet what the DB will return when the imported data
        // is read back into KNIME
        return new PortObjectSpec[]{null};
    }

    /**
     * Checks the given DatabaseSettings and throws <code>InvalidSettingsException</code>
     *
     * @param inSpecs The input data table specs.
     * @param cp The credential provider
     * @throws InvalidSettingsException
     */
    private void checkDatabaseSettings(final PortObjectSpec[] inSpecs, final CredentialsProvider cp) throws InvalidSettingsException {
        final DataTableSpec tableSpec = (DataTableSpec)inSpecs[1];

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

        for (final String colName : m_settings.partitionColumns()) {
            final DataColumnSpec cs = tableSpec.getColumnSpec(colName);
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

    /**
     * Checks the given DatabaseSettings and throws <code>InvalidSettingsException</code>
     *
     * @param inSpecs The input data table specs.
     * @param cp The credential provider
     * @throws InvalidSettingsException
     */
    private void checkDatabaseConnection(final PortObjectSpec[] inSpecs)
            throws InvalidSettingsException {
        // check database connection
        final DatabaseConnectionSettings connSettings =
                ((DatabaseConnectionPortObjectSpec)inSpecs[2]).getConnectionSettings(getCredentialsProvider());

        if ((connSettings.getJDBCUrl() == null) || connSettings.getJDBCUrl().isEmpty()
                || (connSettings.getDriver() == null) || connSettings.getDriver().isEmpty()) {
            throw new InvalidSettingsException("No valid database connection provided via second input port");
        }

    }

    /**
     * Checks the given Settings for File upload and throws <code>InvalidSettingsException</code> if issues are found
     *
     * @param inSpecs The input data table specs.
     * @param cp The credential provider
     * @throws InvalidSettingsException
     */
    private void checkUploadSettings(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {

        final ConnectionInformationPortObjectSpec connSpec = (ConnectionInformationPortObjectSpec) inSpecs[0];
        final ConnectionInformation connInfo = connSpec.getConnectionInformation();
        final CredentialsProvider cp = getCredentialsProvider();
        final DatabaseConnectionPortObjectSpec dbSpec = (DatabaseConnectionPortObjectSpec) inSpecs[2];
        final DatabaseConnectionSettings dbSettings = dbSpec.getConnectionSettings(cp);


        // Check if the port object has connection information
        if (connInfo == null) {
            throw new InvalidSettingsException("No connection information available");
        }

        if (StringUtils.isBlank(m_settings.targetFolder())) {
            throw new InvalidSettingsException("No target folder for data upload provided");
        }

        if (HDFSRemoteFileHandler.isSupportedConnection(connInfo)
                && !connInfo.useKerberos()
                && (connInfo.getUser() != null) && !connInfo.getUser().isEmpty()
                && (dbSettings.getUserName(cp) != null) && !dbSettings.getUserName(cp).isEmpty()
                && !connInfo.getUser().equals(dbSettings.getUserName(cp))) {

            setWarningMessage("Different HDFS and database user "+ connInfo.getUser() + "/" + dbSettings.getUserName(cp) +" found, this might result in permission problems.");
        }

        if (HDFSLocalRemoteFileHandler.isSupportedConnection(connInfo) && !this.loader.isImpala()) {
            validateLocalTargetFolder(m_settings.targetFolder());
        }
    }

    /**
     * BD-729: Throws an error if target folder contains characters that needs to be URI encoded, because Spark 2.3
     * Thriftserver does not support them (bug).
     *
     * TODO BD-803: Remove this after upgrade of local big data environment to Spark 2.4.
     *
     * @param path
     * @throws InvalidSettingsException
     */
    public static void validateLocalTargetFolder(final String path) throws InvalidSettingsException {
        final String preMsg = "Local Spark 2.3 Thriftserver (Hive) does not support file import\n"
                + "from a path with special characters (e.g. space characters).\n";

        try {
            final URI uri = new URI("file", "", path, null);
            final int index = StringUtils.indexOfDifference("file://" + path, uri.toString()) - "file://".length();

            if (index >= 0 && index < path.length() && path.charAt(index) == ' ') {
                throw new InvalidSettingsException(String.format(
                    preMsg + "Unsupported space character at index %d detected.", index + 1));

            } else if(path.indexOf('%') > -1) {
                throw new InvalidSettingsException(String.format(
                    preMsg + "Unsupported percentage character at index %d detected.", path.indexOf('%') + 1));

            } else if (index >= 0) {
                throw new InvalidSettingsException(String.format(
                    preMsg + "Unsupported character at index %d detected: %c", index + 1, path.charAt(index)));
            }
        } catch (URISyntaxException e) {
            // never happen
        }
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        exec.setProgress(null);
        final ConnectionInformation connInfo = ((ConnectionInformationPortObject)inObjects[0]).getConnectionInformation();
        final BufferedDataTable table = (BufferedDataTable)inObjects[1];
        final DatabaseConnectionPortObject dbObj = (DatabaseConnectionPortObject)inObjects[2];
        final CredentialsProvider cp = getCredentialsProvider();
        final DatabaseConnectionSettings dbConn = dbObj.getConnectionSettings(cp);
        //create temporary file
        final File dataFile = writeDataToFile(table, exec.createSubProgress(0.1), dbConn.getTimeZone());
        final ConnectionMonitor<?> connMonitor = new ConnectionMonitor<>();
        RemoteFile<? extends Connection> remoteFile = null;
        try {
            //upload temporary file to remote FS
            remoteFile = loader.uploadFile(dataFile, connInfo, (ConnectionMonitor<? extends Connection>)connMonitor,
                exec.createSubExecutionContext(0.2), m_settings);
            exec.checkCanceled();
            final DataTableSpec tableSpec = table.getDataTableSpec();
            final List<String> normalColumns = new ArrayList<>();
            for (final DataColumnSpec cs : tableSpec) {
                normalColumns.add(cs.getName());
            }
            loader.importData(remoteFile, normalColumns, dbConn,
                exec.createSubExecutionContext(0.5), m_settings, cp);
        } finally {
            Exception er = null;
            if (remoteFile != null) {
                try {
                    if (remoteFile.exists() && !remoteFile.delete()) {
                        setWarningMessage("Could not delete temporary import file on server");
                    }
                } catch (final Exception e) {
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
        final DatabaseQueryConnectionSettings querySettings =
                new DatabaseQueryConnectionSettings(dbConn, "SELECT * FROM " + m_settings.tableName());
        final DBReader conn =
                DatabaseUtility.getUtility(dbConn.getDatabaseIdentifier()).getReader(querySettings);

        final DataTableSpec tableSpec = conn.getDataTableSpec(cp);
        final DatabasePortObjectSpec outSpec = new DatabasePortObjectSpec(tableSpec, querySettings);
        final DatabasePortObject retVal = new DatabasePortObject(outSpec);

        return new PortObject[]{retVal};
    }


    /**
     * Writes a {@link BufferedDataTable} to a temporary .csv file
     *
     * @param table The {@link BufferedDataTable} to be written
     * @param execMon
     * @param timeZone timeZone for DateTime conversion
     * @return  Returns the written File
     * @throws IOException
     * @throws CanceledExecutionException
     */
    private File writeDataToFile(final BufferedDataTable table, final ExecutionMonitor execMon, final TimeZone timeZone)
            throws IOException, CanceledExecutionException {
        final String tempFileName;
        //Just for possible debugging purposes us different file names
        if(loader.isImpala()){
            tempFileName =  "knime2impala_" + UUID.randomUUID().toString().replace('-', '_');
        }else{
            tempFileName = "hive-import"+ UUID.randomUUID().toString().replace('-', '_') ;
        }
        final File tempFile = FileUtil.createTempFile(tempFileName,  ".csv");

        LOGGER.debug("Start writing KNIME table to temporary file " + tempFile.getPath());
        final double max = table.size();
        LOGGER.debug("Table structure " + table.getSpec());
        LOGGER.debug("No of rows to write " + max);


        try (BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tempFile),"UTF-8"))) {
            writeRows(table, execMon, timeZone, out);
        } catch (IOException | CanceledExecutionException ex) {
            LOGGER.debug("Exception while writing to temporary file: " + ex.getMessage());
            Files.deleteIfExists(tempFile.toPath());
            throw ex;
        }
        LOGGER.debug("Temporary file successful created at " + tempFile.getPath());
        return tempFile;
    }

    /**
     *Converts the rows of the given table to String and writes them to <code>out </out>
     *
     * @param table table to be written
     * @param execMon {@link ExecutionMonitor} for progress updates
     * @param timeZone for DateTime conversion
     * @param out the BufferedWriter to write to
     * @throws CanceledExecutionException
     * @throws IOException
     */
    private void writeRows(
        final BufferedDataTable table,
        final ExecutionMonitor execMon,
        final TimeZone timeZone,
        final BufferedWriter out)
                throws CanceledExecutionException, IOException {
        final double max = table.size();
        long count = 0;
        final SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyy-MM-dd HH:mm:ss.SSS");
        final SimpleDateFormat dateFormat = new SimpleDateFormat("yyy-MM-dd");
        for (final DataRow row : table) {
            execMon.setProgress(count++ / max, "Writing table to temporary file (" + count + " rows)");
            execMon.checkCanceled();
            for (int i = 0; i < row.getNumCells(); i++) {
                final DataCell c = row.getCell(i);
                if (c.isMissing()) {
                    out.write("\\N");
                } else {
                    final String s;
                    if (c instanceof DateAndTimeValue) {
                        final DateAndTimeValue d = (DateAndTimeValue) c;
                        final long corrDate =
                                d.getUTCTimeInMillis() - timeZone.getOffset(d.getUTCTimeInMillis());
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
                        throw new IOException(
                            "Line breaks in cell contents are not supported (row '"
                                    + row.getKey()
                                    + "', column '"
                                    + table.getDataTableSpec().getColumnSpec(i).getName()
                                    + "')");
                    }
                    final String sr = s.replace("\t", "\\\t"); // replace column delimiter
                    out.write(sr);
                }
                if (i < (row.getNumCells() - 1)) {
                    out.write('\t');
                } else {
                    out.write('\n');
                }
            }
        }
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
        final LoaderSettings hs = new LoaderSettings();
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
