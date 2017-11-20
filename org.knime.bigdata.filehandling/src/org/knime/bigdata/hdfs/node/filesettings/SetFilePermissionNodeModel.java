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
 *   Created on 07.08.2014 by koetter
 */
package org.knime.bigdata.hdfs.node.filesettings;

import java.io.File;
import java.io.IOException;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObject;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.RemoteFileFactory;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.uri.URIDataValue;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import org.knime.bigdata.hdfs.filehandler.HDFSConnection;
import org.knime.bigdata.hdfs.filehandler.HDFSRemoteFile;
import org.knime.bigdata.hdfs.filehandler.HDFSRemoteFileHandler;

/**
 *
 * @author Tobias Koetter, KNIME AG, Zurich, Switzerland
 */
public class SetFilePermissionNodeModel extends NodeModel {

    private final SettingsModelString m_filePermission = createFilePermissionModel();

    private final SettingsModelString m_col = createURIColModel();

    /**
     *
     */
    public SetFilePermissionNodeModel() {
        super(new PortType[] {ConnectionInformationPortObject.TYPE, BufferedDataTable.TYPE},
            new PortType[] {BufferedDataTable.TYPE});
    }

    /**
     * @return the column name model
     */
    static SettingsModelString createURIColModel() {
        return new SettingsModelString("columnName", null);
    }

    /**
     * @return the file permission pattern model
     */
    static SettingsModelString createFilePermissionModel() {
        return new SettingsModelString("filePermission", "-rwxrwxrwx");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        HDFSRemoteFileHandler.LICENSE_CHECKER.checkLicenseInNode();
        final ConnectionInformationPortObjectSpec object = (ConnectionInformationPortObjectSpec)inSpecs[0];
        final ConnectionInformation connInfo = object.getConnectionInformation();
        // Check if the port object has connection information
        if (connInfo == null) {
            throw new InvalidSettingsException("No connection information available");
        }
        if (!HDFSRemoteFileHandler.HDFS_PROTOCOL.getName().equals(connInfo.getProtocol()) 
        		&& !HDFSRemoteFileHandler.WEBHDFS_PROTOCOL.getName().equals(connInfo.getProtocol())) {
            throw new InvalidSettingsException("HDFS/webHDFS connection required");
        }
        final DataTableSpec tableSpec = (DataTableSpec) inSpecs[1];
        if (m_col.getStringValue() == null) {
            //preset the column with the first URI column
            for (DataColumnSpec colSpec : tableSpec) {
                if (colSpec.getType().isCompatible(URIDataValue.class)) {
                    m_col.setStringValue(colSpec.getName());
                    setWarningMessage("Preset URI column to " + colSpec.getName());
                    break;
                }
            }
        }
        final String colName = m_col.getStringValue();
        if (colName == null) {
            throw new InvalidSettingsException("Input table does not contain a URI column");
        }
        DataColumnSpec columnSpec = tableSpec.getColumnSpec(colName);
        if (columnSpec == null) {
            throw new InvalidSettingsException("Input table does not contain a URI column with name: " + colName);
        }
        if (!columnSpec.getType().isCompatible(URIDataValue.class)) {
            throw new InvalidSettingsException("Selected column with name: " + colName + " is not a URI");
        }
        return new PortObjectSpec[] {tableSpec};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        final ConnectionInformationPortObject con = (ConnectionInformationPortObject)inObjects[0];
        final BufferedDataTable table = (BufferedDataTable)inObjects[1];
        final int idx = table.getSpec().findColumnIndex(m_col.getStringValue());
        final ConnectionMonitor<HDFSConnection> monitor = new ConnectionMonitor<>();
        int rowCounter= 0;
        for (DataRow row : table) {
            exec.checkCanceled();
            final int rowCount = table.getRowCount();
            exec.setProgress(rowCounter++ / rowCount, "Processing file" + rowCounter + " of " + rowCount);
            DataCell cell = row.getCell(idx);
            if (cell.isMissing()) {
                continue;
            }
            if (cell instanceof URIDataValue) {
                URIDataValue uriCell = (URIDataValue)cell;
                final HDFSRemoteFile targetFile = RemoteFileFactory.<HDFSConnection, HDFSRemoteFile>createRemoteFile(
                    uriCell.getURIContent().getURI(), con.getConnectionInformation(), monitor);
                targetFile.setPermission(m_filePermission.getStringValue());
            }
        }
        return new PortObject[] {table};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec) throws IOException,
        CanceledExecutionException {
        //nothing to load
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec) throws IOException,
        CanceledExecutionException {
        //nothing to save
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_filePermission.saveSettingsTo(settings);
        m_col.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        final String permission =
                ((SettingsModelString)m_filePermission.createCloneWithValidatedValue(settings)).getStringValue();
        if (permission == null || permission.isEmpty()) {
            throw new InvalidSettingsException("File permission must not be empty");
        }
        final String col = ((SettingsModelString)m_col.createCloneWithValidatedValue(settings)).getStringValue();
        if (col == null || col.isEmpty()) {
            throw new InvalidSettingsException("URI column must not be empty");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_filePermission.loadSettingsFrom(settings);
        m_col.loadSettingsFrom(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reset() {
        // nothing to reset
    }

}
