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
package org.knime.bigdata.database.databricks;

import java.util.Optional;

import org.knime.bigdata.database.hive.HiveDriverLocator;
import org.knime.database.DBType;
import org.knime.database.driver.DBDriverDefinition;
import org.knime.database.driver.DBDriverRegistry;
import org.knime.database.driver.DBDriverWrapper;

/**
 * This class contains the Databricks driver definition that use the open source Hive driver.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class DatabricksDBDriverLocator extends HiveDriverLocator {

    /** Driver id. */
    @SuppressWarnings("hiding")
    public static final String DRIVER_ID = "databricksHive";

    private static final DBType DB_TYPE = Databricks.DB_TYPE;

    private static final String HIVE_DRIVER_CLASS_PREFIX = "org.apache.hive.jdbc.";

    private static final String SIMBA_DRIVER_CLASS_PREFIX = "com.simba.spark.";

    private static final String DATABRCIKS_DRIVER_CLASS_PREFIX = "com.databricks.";

    @Override
    public String getDriverId() {
        return DRIVER_ID;
    }

    @Override
    public String getDriverName() {
        return "Legacy Hive JDBC Driver (for Databricks)";
    }

    @Override
    public DBType getDBType() {
        return DB_TYPE;
    }

    @Override
    public String getURLTemplate() {
        return "jdbc:hive2://<host>:<port>/default";
    }

    /**
     * @return latest Databricks, Simba or Hive driver ID or {@code null}
     */
    public static String getLatestSimbaOrHiveDriverID() {
        return getLatestDriverID(DATABRCIKS_DRIVER_CLASS_PREFIX).orElse(getLatestDriverID(SIMBA_DRIVER_CLASS_PREFIX)
            .orElse(getLatestDriverID(HIVE_DRIVER_CLASS_PREFIX).orElse(null)));
    }

    private static Optional<String> getLatestDriverID(final String driverClassPrefix) {
        return DBDriverRegistry.getInstance().getDrivers(DB_TYPE).parallelStream()
            .map(DBDriverWrapper::getDriverDefinition)
            .filter(d -> d.getDriverClass().toLowerCase().startsWith(driverClassPrefix))
            .sorted((a, b) -> b.getVersion().compareTo(a.getVersion()))
            .findFirst()
            .map(DBDriverDefinition::getId);
    }

    /**
     * @param jdbcUrl JDBC URL to verify
     * @return {@code true} if JDBC URL starts with jdbc:hive2
     */
    public static boolean isHiveConnection(final String jdbcUrl) {
        return jdbcUrl.toLowerCase().startsWith("jdbc:hive2:");
    }

    /**
     * @param jdbcUrl JDBC URL to verify
     * @return {@code true} if JDBC URL starts with jdbc:spark
     */
    public static boolean isSimbaConnection(final String jdbcUrl) {
        return jdbcUrl.toLowerCase().startsWith("jdbc:databricks:") || jdbcUrl.toLowerCase().startsWith("jdbc:spark:");
    }
}
