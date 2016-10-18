/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
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
package com.knime.bigdata.hive.utility;

import java.sql.Driver;
import java.util.Set;

import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.database.DatabaseConnectionSettings;
import org.knime.core.node.port.database.DatabaseUtility;

/**
 *
 * @author Sascha Wolke, KNIME.com
 */
public class HiveDriverDetector {
    private static final NodeLogger LOGGER = NodeLogger.getLogger(HiveDriverDetector.class);

    private static final String CLOUDERA_DRIVER_NAME = "com.cloudera.hive.jdbc41.HS2Driver";

    private static final String AMAZON_EMR_DRIVER_NAME_REGEX = "com\\.amazon\\.hive\\.jdbc[0-9]+\\.HS2Driver";

    /**
     * Searches for drivers and returns the preferred driver (external drivers are preferred).
     *
     * @return the driver class of the preferred driver to use
     */
    public static String getDriverName() {
        final Set<String> externalDriver = DatabaseUtility.getJDBCDriverClasses();
        String driverName;

        if (externalDriver.contains(CLOUDERA_DRIVER_NAME)) {
            driverName = CLOUDERA_DRIVER_NAME;
            LOGGER.debug("Using Cloudera Hive driver: " + driverName);

        } else if ((driverName = containsDriver(externalDriver, AMAZON_EMR_DRIVER_NAME_REGEX)) != null) {
            LOGGER.debug("Using Amazon EMR Hive driver: " + driverName);

        } else {
            driverName = HiveUtility.DRIVER;
            LOGGER.debug("Using open source Hive driver: " + driverName);
        }

        return driverName;
    }

    /**
     *
     * @param driverName
     * @return a pretty driver name for display purposes
     */
    public static String mapToPrettyDriverName(final String driverName) {
        String versionInfo = "";
        try {
            final DatabaseConnectionSettings settings = new DatabaseConnectionSettings();
            settings.setDriver(driverName);
            final Driver driver = DatabaseUtility.getUtility(HiveUtility.DATABASE_IDENTIFIER).getConnectionFactory().getDriverFactory().getDriver(settings);
            versionInfo = String.format(", version: %d.%d", driver.getMajorVersion(), driver.getMinorVersion());
        } catch(Exception e) {
        }

        if (driverName.equals(CLOUDERA_DRIVER_NAME)) {
            return String.format("Cloudera Hive Driver (%s%s)", driverName, versionInfo);
        } else if (driverName.matches(AMAZON_EMR_DRIVER_NAME_REGEX)) {
            return String.format("Amazon EMR Hive Driver (%s%s)", driverName, versionInfo);
        } else if (driverName.equals(HiveUtility.DRIVER)) {
            return String.format("Open-Source Hive Driver (%s%s)", driverName, versionInfo);
        } else {
            return driverName;
        }
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
