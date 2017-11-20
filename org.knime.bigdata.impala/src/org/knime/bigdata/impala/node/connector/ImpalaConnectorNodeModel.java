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
package org.knime.bigdata.impala.node.connector;

import java.io.File;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.sql.SQLException;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.knime.bigdata.impala.utility.ImpalaDriverDetector;
import org.knime.bigdata.impala.utility.ImpalaUtility;
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

/**
 * Model for the Impala connector node.
 *
 * @author Tobias Koetter, KNIME AG, Zurich, Switzerland
 */
class ImpalaConnectorNodeModel extends NodeModel {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(ImpalaConnectorNodeModel.class);

    private final ImpalaConnectorSettings m_settings = new ImpalaConnectorSettings();

    ImpalaConnectorNodeModel() {
        super(new PortType[0], new PortType[]{DatabaseConnectionPortObject.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        ImpalaUtility.LICENSE_CHECKER.checkLicenseInNode();

        if (!ImpalaDriverDetector.getDriverName().equals(m_settings.getDriver())) {
            setWarningMessage(String.format("Using %s\ninstead of %s",
                ImpalaDriverDetector.mapToPrettyDriverName(ImpalaDriverDetector.getDriverName()),
                ImpalaDriverDetector.mapToPrettyDriverName(m_settings.getDriver())));
            m_settings.setDriver(ImpalaDriverDetector.getDriverName());
        }

        if ((m_settings.getHost() == null) || m_settings.getHost().isEmpty()) {
            throw new InvalidSettingsException("No hostname for database server given");
        }

        final String userName = m_settings.getUserName(getCredentialsProvider());
        if (!m_settings.useKerberos() && (m_settings.getCredentialName() == null)
                && ((userName == null) || userName.isEmpty())) {
            throw new InvalidSettingsException("No credentials or username for authentication given");
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
     * @param settings the {@link ImpalaConnectorSettings} to use
     * @return the jdbc url
     */
    static String getJDBCURL(final ImpalaConnectorSettings settings) {
        final String pwd = settings.getPassword(null);
        final StringBuilder buf = new StringBuilder();
        if (ImpalaDriverDetector.clouderaDriverAvailable()) {
            buf.append("jdbc:impala://" + settings.getHost() + ":" + settings.getPort());
            //append database
            buf.append("/" + settings.getDatabaseName());
        } else {
            buf.append("jdbc:hive2://" + settings.getHost() + ":" + settings.getPort());
            //append database
            buf.append("/" + settings.getDatabaseName());
            if (!settings.useKerberos() && (pwd == null || pwd.trim().length() == 0)) {
                buf.append(";auth=noSasl");
            }
        }
        final String parameter = settings.getParameter();
        if (parameter != null && !parameter.trim().isEmpty()) {
            if (!parameter.startsWith(";")) {
                buf.append(";");
            }
            buf.append(parameter);
        }
        final String jdbcUrl = buf.toString();
        LOGGER.debug("Using jdbc url: " + jdbcUrl);
        return jdbcUrl;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        DatabaseConnectionPortObjectSpec spec = createSpec();
        try {
            spec.getConnectionSettings(getCredentialsProvider()).createConnection(getCredentialsProvider());
        } catch (InvalidKeyException | BadPaddingException | IllegalBlockSizeException | InvalidSettingsException
                | SQLException | IOException ex) {
            Throwable cause = ExceptionUtils.getRootCause(ex);
            if (cause == null) {
                cause = ex;
            }
            String errorMessage = cause.getMessage();
            if (errorMessage == null) {
                errorMessage = "Maybe invalid user name and/or password";
            }
            throw new SQLException("Could not create connection to database: " + errorMessage, ex);
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
        final ImpalaConnectorSettings s = new ImpalaConnectorSettings();
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
