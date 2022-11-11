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
import java.util.regex.Pattern;

import org.knime.bigdata.commons.config.CommonConfigContainer;
import org.knime.bigdata.commons.hadoop.UserGroupUtil;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.database.connection.CachedConnectionFactory;
import org.knime.core.node.port.database.connection.DBDriverFactory;

/**
 * {@link CachedConnectionFactory} implementation for Hive and Impala that supports Kerberos authentication.
 *
 * @author Tobias Koetter, KNIME.com
 * @since 3.6
 */
@Deprecated
public class KerberosConnectionFactory extends CachedConnectionFactory {

    private static final String FALLBACK_IMPERSONATION_PARAMETER =
        String.format("hive.server2.proxy.user=%s", CommonConfigContainer.JDBC_IMPERSONATION_PLACEHOLDER);

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

            final String effectiveJdcbUrl;
            final Optional<String> userToImpersonate = CommonConfigContainer.getInstance().getUserToImpersonate();
            if (userToImpersonate.isPresent()) {
                effectiveJdcbUrl = appendImpersonationParameter(jdbcUrl, userToImpersonate.get());
            } else {
                effectiveJdcbUrl = jdbcUrl;
            }

            return UserGroupUtil.runWithKerberosUGI((ugi) -> {
                LOGGER.debug("Create JDBC connection with Kerberos user: " + ugi.toString());
                final Properties props = createConnectionProperties(ugi.getShortUserName(), null);
                return ugi.doAs((PrivilegedExceptionAction<Connection>)() ->
                d.connect(effectiveJdcbUrl, props));
            });
        } catch (SQLException e) {
            // don't rewrap SQLException
            throw e;
        } catch (Exception e) {
            throw new SQLException(e.getMessage(), e);
        }
    }

    private static String appendImpersonationParameter(final String jdbcUrl, final String userToImpersonate) {

        String param = FALLBACK_IMPERSONATION_PARAMETER;
        if (CommonConfigContainer.getInstance().useJDBCImpersonationParameter()) {
            param = CommonConfigContainer.getInstance().getJDBCImpersonationParameter();
        }

        // this checks that the user does not maliciously uses the impersonation parameter
        final String searchString = param.replace(CommonConfigContainer.JDBC_IMPERSONATION_PLACEHOLDER, "");
        if (jdbcUrl.contains(searchString)) {
            throw new IllegalArgumentException("JDBC URL must not contain user impersonation parameter");
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
