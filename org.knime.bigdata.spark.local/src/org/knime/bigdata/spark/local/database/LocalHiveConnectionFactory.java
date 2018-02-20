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
 *   Created on 06.02.2018 by oole
 */
package org.knime.bigdata.spark.local.database;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.database.connection.CachedConnectionFactory;
import org.knime.core.node.port.database.connection.DBDriverFactory;

/**
 * {@link CachedConnectionFactory} for the local thrift server connection.
 *
 * @author Ole Ostergaard, KNIME AG, Konstanz, Germany
 *
 */
public class LocalHiveConnectionFactory extends CachedConnectionFactory {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(LocalHiveConnectionFactory.class);

    /**
     * @param driverFactory the {@link DBDriverFactory} to get the {@link Driver}
     */
    public LocalHiveConnectionFactory(final DBDriverFactory driverFactory) {
        super(driverFactory);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Connection createConnection(final String jdbcUrl, final String user, final String pass,
        final boolean useKerberos, final Driver d) throws SQLException {
    	if (useKerberos) {
    		final SQLException e = new SQLException("Kerberos authentication not supported on local Hive connection.");
    		LOGGER.error("Kerberos authentication not supported on local Hive connection.", e);
    		throw e;
    	}
    	final Properties props = createConnectionProperties(user, pass);
    	final Connection connection  = new LocalHiveConnection(jdbcUrl, props);
        return connection;
    }
}
