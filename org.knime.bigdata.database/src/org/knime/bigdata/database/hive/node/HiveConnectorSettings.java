package org.knime.bigdata.database.hive.node;

import org.knime.bigdata.database.hive.Hive;

/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   05.04.2019 (Mareike Hoeger, KNIME GmbH, Konstanz, Germany): created
 */

import org.knime.database.DBType;
import org.knime.database.dialect.DBSQLDialectFactory;
import org.knime.database.dialect.DBSQLDialectRegistry;
import org.knime.database.driver.DBDriverRegistry;
import org.knime.database.driver.DBDriverWrapper;
import org.knime.database.node.connector.server.ServerDBConnectorSettings;


/**
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 *
 */
public class HiveConnectorSettings extends ServerDBConnectorSettings {

    private static final DBType DB_TYPE = Hive.DB_TYPE;

    private static final int DEFAULT_PORT = 10000;

    private static final DBSQLDialectFactory DEFAULT_DIALECT_FACTORY =
        DBSQLDialectRegistry.getInstance().getDefaultFactoryFor(DB_TYPE);

    /**
     * Hive Connector settings
     */
    protected HiveConnectorSettings() {
        super("hive-connection");

        setDBType(DB_TYPE.getId());

        setDialect(DEFAULT_DIALECT_FACTORY.getId());

        final DBDriverWrapper defaultDriver = DBDriverRegistry.getInstance().getLatestDriver(DB_TYPE);
        setDriver(defaultDriver == null ? null : defaultDriver.getDriverDefinition().getId());

        setPort(DEFAULT_PORT);
    }

    @Override
    protected String createJdbcUrl() {
        final String host = getHost();
        final int port = getPort();
        final String dbName = getDatabaseName();

        return String.format("jdbc:hive2://%s:%s/%s", host, port, dbName);
    }


    @Override
    protected String getDatabaseTypeId() {
        return DB_TYPE.getId();
    }

    @Override
    public boolean isDatabaseNameMandatory() {
        return false;
    }

}
