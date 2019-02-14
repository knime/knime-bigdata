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
package org.knime.bigdata.hive.utility;

import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import org.knime.bigdata.commons.config.CommonConfigContainer;
import org.knime.bigdata.commons.hadoop.UserGroupUtil;
import org.knime.bigdata.commons.hadoop.UserGroupUtil.UserGroupInformationCallback;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.database.connection.CachedConnectionFactory;
import org.knime.core.node.port.database.connection.DBDriverFactory;

/**
 * {@link CachedConnectionFactory} implementation for Hive and Impala that supports Kerberos authentication.
 *
 * @author Tobias Koetter, KNIME.com
 * @since 3.6
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
            // use AtomicRef to be able to lazily get the jdbc URL inside the ugiCallback
            final AtomicReference<String> jdcbUrlRef = new AtomicReference<>();

            final UserGroupInformationCallback<Connection> ugiCallback = (ugi) -> {
                LOGGER.debug("Create JDBC connection with Kerberos user: " + ugi.toString());
                final Properties props = createConnectionProperties(ugi.getShortUserName(), null);
                return ugi.doAs((PrivilegedExceptionAction<Connection>)() -> d.connect(jdcbUrlRef.get(), props));
            };

            final Optional<String> userToImpersonate = CommonConfigContainer.getInstance().getUserToImpersonate();
            if (CommonConfigContainer.getInstance().useJDBCImpersonationParameter() && userToImpersonate.isPresent()) {
                LOGGER.debug("Using JDBC impersonation parameter instead of proxy user on KNIME Server");
                jdcbUrlRef.set(appendImpersonationParameter(jdbcUrl, userToImpersonate.get()));
                return UserGroupUtil.runWithKerberosUGI(ugiCallback);
            } else {
                jdcbUrlRef.set(jdbcUrl);
                return UserGroupUtil.runWithProxyUserUGIIfNecessary(ugiCallback);
            }
        } catch (Exception e) {
            final String errMsg = "Exception creating Kerberos based JDBC connection. Error: " + e.getMessage();
            LOGGER.error(errMsg, e);
            throw new SQLException(errMsg, e);
        }
    }

    private static String appendImpersonationParameter(final String jdbcUrl, final String userToImpersonate) {
        final String param = CommonConfigContainer.getInstance().getJDBCImpersonationParameter();
        LOGGER.debug("JDBC impersonation parameter: " + param);
        LOGGER.debug("Original JDBC URL: " + jdbcUrl);

        //this checks that the user does not maliciously uses the impersonation parameter
        final String searchString = param.replace(CommonConfigContainer.JDBC_IMPERSONATION_PLACEHOLDER, "");
        if (jdbcUrl.contains(searchString)) {
            throw new IllegalArgumentException("JDBC URL must not contain Kerberos impersonation parameter");
        }

        final String replacedParam = param.replaceAll(
            Pattern.quote(CommonConfigContainer.JDBC_IMPERSONATION_PLACEHOLDER), userToImpersonate);
        final StringBuilder buf = new StringBuilder(jdbcUrl);
        if (!jdbcUrl.endsWith(";")) {
            buf.append(";");
        }
        buf.append(replacedParam);
        final String newJDBCurl = buf.toString();
        LOGGER.debug("JDBC URL with impersonation parameter: " + newJDBCurl);
        return newJDBCurl;
    }
}
