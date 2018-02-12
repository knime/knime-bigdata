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
package org.knime.bigdata.commons.security.kerberos;

import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.UserGroupInformation;
import org.knime.bigdata.commons.config.CommonConfigContainer;
import org.knime.bigdata.commons.config.eclipse.CommonPreferenceInitializer;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.database.connection.CachedConnectionFactory;
import org.knime.core.node.port.database.connection.DBDriverFactory;
import org.knime.core.node.workflow.NodeContext;
import org.knime.core.util.KNIMERuntimeContext;

/**
 * {@link CachedConnectionFactory} implementation that supports Kerberos authentication.
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
            final Configuration conf = new Configuration();
            conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, AuthMethod.KERBEROS.name());
            final CommonConfigContainer configContainer = CommonConfigContainer.getInstance();
            if (configContainer.hasCoreSiteConfig()) {
                LOGGER.debug("Adding core site from config");
                conf.addResource(configContainer.getCoreSiteConfig());
            }
            if (configContainer.hasHdfsSiteConfig()) {
                LOGGER.debug("Adding hdfs site from config");
                conf.addResource(configContainer.getHdfsSiteConfig());
            }
            final UserGroupInformation ugi;
            final String jdbcImpersonationURL;
            if (CommonConfigContainer.getInstance().useJDBCImpersonationParameter()
                    && KNIMERuntimeContext.INSTANCE.runningInServerContext()) {
                LOGGER.debug("Using JDBC impersonation parameter instead of proxy user on KNIME Server");
                jdbcImpersonationURL = appendImpersonationParameter(jdbcUrl);
                ugi = UserGroupUtil.getKerberosTGTUser(conf);
            } else {
                ugi = UserGroupUtil.getKerberosUser(conf);
                jdbcImpersonationURL = jdbcUrl;
            }
            final Properties props = createConnectionProperties(ugi.getShortUserName(), null);
            LOGGER.debug("Create jdbc connection with Kerberos user: " + ugi.toString());
            final Connection con = ugi.doAs(new PrivilegedExceptionAction<Connection>() {
                @Override
                public Connection run() throws Exception {
                    try {
                        return d.connect(jdbcImpersonationURL, props);
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

    private String appendImpersonationParameter(final String jdbcUrl) {
        final String param = CommonConfigContainer.getInstance().getJDBCImpersonationParameter();
        LOGGER.debug("JDBC impersonation parameter: " + param);
        LOGGER.debug("Original JDBC URL: " + jdbcUrl);
        final String searchString = param.replace(CommonPreferenceInitializer.JDBC_IMPERSONATION_PLACEHOLDER, "");
        if (jdbcUrl.contains(searchString)) {
            throw new IllegalArgumentException("JDBC URL must not contain Kerberos impersonation parameter");
        }
        final Optional<String> wfUser = NodeContext.getWorkflowUser();
        if (wfUser.isPresent()) {
            final String replacedParam = param.replaceAll(
                Pattern.quote(CommonPreferenceInitializer.JDBC_IMPERSONATION_PLACEHOLDER), wfUser.get());
            final StringBuilder buf = new StringBuilder(jdbcUrl);
            if (!jdbcUrl.endsWith(";")) {
                buf.append(";");
            }
            buf.append(replacedParam);
            final String newJDBCurl = buf.toString();
            LOGGER.debug("JDBC URL with impersonation parameter: " + newJDBCurl);
            return newJDBCurl;
        }
        LOGGER.warn("No workflow user found. Using original JDBC URL");
        return jdbcUrl;
    }
}
