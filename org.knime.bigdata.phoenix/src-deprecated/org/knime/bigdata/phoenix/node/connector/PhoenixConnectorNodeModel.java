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
 *   Created on 06.05.2014 by thor
 */
package org.knime.bigdata.phoenix.node.connector;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.knime.bigdata.phoenix.utility.PhoenixUtility;
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

/**
 * Model for the Impala connector node.
 *
 * @author Tobias Koetter, KNIME AG, Zurich, Switzerland
 */
class PhoenixConnectorNodeModel extends NodeModel {
    private final PhoenixConnectorSettings m_settings = new PhoenixConnectorSettings();

    PhoenixConnectorNodeModel() {
        super(new PortType[0], new PortType[]{DatabaseConnectionPortObject.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        m_settings.setDriver(PhoenixUtility.DRIVER);

        if ((m_settings.getHost() == null) || m_settings.getHost().isEmpty()) {
            throw new InvalidSettingsException("No hostname for database server given");
        }
        if ((m_settings.getCredentialName() == null)
            && ((m_settings.getUserName(getCredentialsProvider()) == null) || m_settings.getUserName(
                getCredentialsProvider()).isEmpty())) {
            throw new InvalidSettingsException("No credentials or username for authentication given");
        }
        return new PortObjectSpec[]{createSpec()};
    }

    private DatabaseConnectionPortObjectSpec createSpec() {
        final DatabaseConnectionSettings s = new DatabaseConnectionSettings(m_settings);
        //set the jdbc url again in the case that the settings have been changed by variables
        s.setJDBCUrl(getJDBCURL(m_settings.getHost(), m_settings.getPort(), m_settings.getDatabaseName()));
        final DatabaseConnectionPortObjectSpec spec = new DatabaseConnectionPortObjectSpec(s);
        return spec;
    }

    /**
     * @param host the host
     * @param port the port
     * @param dbName the db name
     * @return the jdbc url
     */
    static String getJDBCURL(final String host, final int port, final String dbName) {
        return "jdbc:phoenix:" + host + ":" + port + ((dbName == null || dbName.isEmpty()) ? "" : ":/" + dbName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        final DatabaseConnectionPortObjectSpec spec = createSpec();
        try {
            spec.getConnectionSettings(getCredentialsProvider()).execute(getCredentialsProvider(),
                conn -> {return conn != null;});
        } catch (SQLException ex) {
            Throwable cause = ExceptionUtils.getRootCause(ex);
            if (cause == null) {
                cause = ex;
            }
            throw new SQLException("Could not create connection to database: " + cause.getMessage(), ex);
        }

        return new PortObject[]{new DatabaseConnectionPortObject(spec)};
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
        m_settings.saveConnection(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        PhoenixConnectorSettings s = new PhoenixConnectorSettings();
        s.validateConnection(settings, getCredentialsProvider());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadValidatedConnection(settings, getCredentialsProvider());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reset() {
    }
}
