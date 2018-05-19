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
 *   Created on 23.06.2016 by koetter
 */
package org.knime.bigdata.phoenix.utility;

import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.knime.bigdata.commons.hadoop.ConfigurationFactory;
import org.knime.bigdata.commons.hadoop.UserGroupUtil;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.database.connection.CachedConnectionFactory;
import org.knime.core.node.port.database.connection.DBDriverFactory;

/**
 * {@link CachedConnectionFactory} implementation for Phoenix that supports Kerberos authentication.
 *
 * @author Tobias Koetter, KNIME.com
 */
public class KerberosConnectionFactory extends CachedConnectionFactory {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(KerberosConnectionFactory.class);

    /**
     * @param driverFactory the {@link DBDriverFactory} to get the {@link Driver}
     */
    public KerberosConnectionFactory(final DBDriverFactory driverFactory) {
        super(driverFactory);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Connection createConnection(final String jdbcUrl, final String user, final String pass,
        final boolean useKerberos, final Driver d)
        throws SQLException {
        if (!useKerberos) {
            return super.createConnection(jdbcUrl, user, pass, useKerberos, d);
        }
        try {
            final Configuration conf = ConfigurationFactory.createBaseConfigurationWithKerberosAuth();
            final UserGroupInformation ugi = UserGroupUtil.getKerberosUser(conf);

            final Properties props = createConnectionProperties(ugi.getShortUserName(), null);
            LOGGER.debug("Create jdbc connection with Kerberos user: " + ugi.toString());
            final Connection con = ugi.doAs(new PrivilegedExceptionAction<Connection>() {
                @Override
                public Connection run() throws Exception {
                    try {
                        return d.connect(jdbcUrl, props);
                    } catch (Exception e) {
                        throw e;
                    }
                }
            });
            return con;
        } catch (Exception e) {
            final String errMsg = "Exception creating Kerberos based jdbc connection. Error: " + e.getMessage();
            LOGGER.error(errMsg, e);
            throw new SQLException(errMsg, e);
        }
    }
}