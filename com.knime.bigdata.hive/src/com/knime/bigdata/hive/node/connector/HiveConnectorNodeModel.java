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
 *   Created on 06.05.2014 by thor
 */
package com.knime.bigdata.hive.node.connector;

import java.io.File;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.sql.SQLException;
import java.util.Set;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;

import org.apache.commons.lang3.exception.ExceptionUtils;
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
import org.knime.core.node.port.database.DatabaseUtility;

import com.knime.bigdata.hive.utility.HiveDriverFactory;
import com.knime.bigdata.hive.utility.HiveUtility;

/**
 * Model for the Hive connector node.
 *
 * @author Thorsten Meinl, KNIME.com, Zurich, Switzerland
 */
class HiveConnectorNodeModel extends NodeModel {
    private static final NodeLogger LOGGER = NodeLogger.getLogger(HiveConnectorNodeModel.class);
    private static final String CLOUDERA_DRIVER_NAME = "com.cloudera.hive.jdbc41.HS2Driver";
    private final HiveConnectorSettings m_settings = new HiveConnectorSettings();

    HiveConnectorNodeModel() {
        super(new PortType[0], new PortType[]{DatabaseConnectionPortObject.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        HiveUtility.LICENSE_CHECKER.checkLicenseInNode();
        final String driverName;
        if (clouderaDriverAvailable()) {
            driverName = CLOUDERA_DRIVER_NAME;
            LOGGER.debug("Cloudera Hive driver found using driver: " + driverName);
        } else {
            driverName = HiveDriverFactory.DRIVER;
        }
        m_settings.setDriver(driverName);

        if ((m_settings.getCredentialName() == null)
                && ((m_settings.getUserName(getCredentialsProvider()) == null) || m_settings.getUserName(
                    getCredentialsProvider()).isEmpty())) {
            throw new InvalidSettingsException("No credentials or username for authentication given");
        }
        if ((m_settings.getHost() == null) || m_settings.getHost().isEmpty()) {
            throw new InvalidSettingsException("No hostname for database server given");
        }

        return new PortObjectSpec[]{createSpec()};
    }

    private DatabaseConnectionPortObjectSpec createSpec() {
        final DatabaseConnectionSettings s = new DatabaseConnectionSettings(m_settings);
        //set the jdbc url again in the case that the settings have been changed by variables
        s.setJDBCUrl(getJDBCURL(m_settings));
        final DatabaseConnectionPortObjectSpec spec = new DatabaseConnectionPortObjectSpec(s);
        return spec;
    }

    /**
     * @return <code>true</code> if the Cloudera driver has been registered
     */
    private static boolean clouderaDriverAvailable() {
        final Set<String> externalDriver = DatabaseUtility.getJDBCDriverClasses();
        return externalDriver.contains(CLOUDERA_DRIVER_NAME);
    }

    static String getJDBCURL(final HiveConnectorSettings settings) {
        final String host = settings.getHost();
        final int port = settings.getPort();
        final String dbName = settings.getDatabaseName();
        final String url = "jdbc:hive2://" + host + ":" + port + "/" + dbName;
        final String pwd = settings.getPassword(null);
        if (clouderaDriverAvailable()) {
            LOGGER.debug("Cloudera Hive driver found using Cloudera url settings.");
            if (pwd != null && !pwd.trim().isEmpty()) {
                //for user name and password authentication
                return url + ";AuthMech=3";
            }
        }
        return url;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        DatabaseConnectionPortObjectSpec spec = createSpec();

        if (DatabaseConnectionSettings.getDatabaseTimeout() < 30) {
            setWarningMessage("Your database timeout (" + DatabaseConnectionSettings.getDatabaseTimeout()
                + " s) is set to a rather low value for Hive. If you experience timeouts increase the value in the"
                + " preference page.");
        }

        try {
            spec.getConnectionSettings(getCredentialsProvider()).createConnection(getCredentialsProvider());
        } catch (InvalidKeyException | BadPaddingException | IllegalBlockSizeException | InvalidSettingsException
                | SQLException | IOException ex) {
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
        HiveConnectorSettings s = new HiveConnectorSettings();
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
