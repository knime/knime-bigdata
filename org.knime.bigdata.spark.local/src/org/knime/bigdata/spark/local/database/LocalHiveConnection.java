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

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.jdbc.HiveDatabaseMetaData;

/**
 * This class extends the {@link HiveConnection}. It overrides
 * {@link HiveConnection#getMetaData()} to pass our wrapped
 * {@link HiveDatabaseMetaData} object. Which fixes the metadata fetching
 * problems for the local thrift server.
 *
 * @author Ole Ostergaard, KNIME AG, Konstanz, Germany
 */
public class LocalHiveConnection extends HiveConnection {

    /**
     * Constructor.
     * 
     * @param uri The JDBC URL.
     * @param info Additional properties to pass to the {@link HiveConnection}.
     * @throws SQLException If something went wroing creating the underlying Hive connection.
     */
	public LocalHiveConnection(String uri, Properties info) throws SQLException {
		super(uri, info);
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		final HiveDatabaseMetaData hiveMetaData = (HiveDatabaseMetaData) super.getMetaData();
		final DatabaseMetaData metaData = new LocalHiveDatabaseMetaData(hiveMetaData);
		return metaData;
	}
}
