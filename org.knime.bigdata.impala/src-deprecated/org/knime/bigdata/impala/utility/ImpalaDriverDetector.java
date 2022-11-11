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
 *   Created on Jul 26, 2016 by sascha
 */
package org.knime.bigdata.impala.utility;

import java.sql.Driver;
import java.util.Set;

import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.database.DatabaseConnectionSettings;
import org.knime.core.node.port.database.DatabaseUtility;

/**
 * Detects registered Impala driver.
 *
 * @author Sascha Wolke, KNIME.com
 */
@Deprecated
public class ImpalaDriverDetector {
    private static final NodeLogger LOGGER = NodeLogger.getLogger(ImpalaDriverDetector.class);

    private static final String CLOUDERA_DRIVER_NAME_REGEX = "com\\.cloudera\\.impala\\.jdbc[0-9]*.Driver";

    /**
     * Searches for drivers and returns preferred driver to use (external drivers are preferred).
     *
     * @return the driver class name of the preferred driver to use.
     */
    public static String getDriverName() {
        final Set<String> externalDriver = DatabaseUtility.getJDBCDriverClasses();
        String driverName;

        if ((driverName = containsDriver(externalDriver, CLOUDERA_DRIVER_NAME_REGEX)) != null) {
            LOGGER.debug("Using Cloudera Impala driver: " + driverName);

        } else {
            driverName = ImpalaUtility.DRIVER;
            LOGGER.debug("Using open source Impala driver: " + driverName);
        }

        return driverName;
    }

    /**
     *
     * @return a pretty driver name for display purposes
     */
    public static String mapToPrettyDriverName(final String driverName) {
        String versionInfo = "";
        try {
            final DatabaseConnectionSettings settings = new DatabaseConnectionSettings();
            settings.setDriver(driverName);
            final Driver driver = DatabaseUtility.getUtility(ImpalaUtility.DATABASE_IDENTIFIER).getConnectionFactory().getDriverFactory().getDriver(settings);
            versionInfo = String.format(", version: %d.%d", driver.getMajorVersion(), driver.getMinorVersion());
        } catch(Exception e) {
        }

        if (driverName.matches(CLOUDERA_DRIVER_NAME_REGEX)) {
            return String.format("Cloudera Impala Driver (%s%s)", driverName, versionInfo);
        } else if (driverName.equals(ImpalaUtility.DRIVER)) {
            return String.format("Open-Source Impala Driver (%s%s)", driverName, versionInfo);
        } else {
            return driverName;
        }
    }

    /**
     * @return <code>true</code> if the Cloudera driver has been registered
     */
    public static boolean clouderaDriverAvailable() {
        return containsDriver(DatabaseUtility.getJDBCDriverClasses(), CLOUDERA_DRIVER_NAME_REGEX) != null;
    }

    private static String containsDriver(final Set<String> drivers, final String driverRegex) {
        for (String driver : drivers) {
            if (driver.matches(driverRegex)) {
                return driver;
            }
        }

        return null;
    }
}
