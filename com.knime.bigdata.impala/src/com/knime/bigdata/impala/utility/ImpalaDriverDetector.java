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
package com.knime.bigdata.impala.utility;

import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.database.DatabaseUtility;

/**
 * Detects registered Impala driver.
 *
 * @author Sascha Wolke, KNIME.com
 */
public class ImpalaDriverDetector {
    private static final NodeLogger LOGGER = NodeLogger.getLogger(ImpalaDriverDetector.class);

    private static final String CLOUDERA_DRIVER_NAME = "com.cloudera.impala.jdbc41.Driver";

    /**
     * Searches for drivers and returns preferred driver to use (external drivers are preferred).
     *
     * @return the driver class name of the preferred driver to use.
     */
    public static String getDriverName() {
        String driverName;

        if (clouderaDriverAvailable()) {
            driverName = CLOUDERA_DRIVER_NAME;
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
        if (driverName.equals(CLOUDERA_DRIVER_NAME)) {
            return String.format("Cloudera Impala Driver (%s)", driverName);
        } else if (driverName.equals(ImpalaUtility.DRIVER)) {
            return String.format("Open-Source Impala Driver (%s)", driverName);
        } else {
            return driverName;
        }
    }

    /**
     * @return <code>true</code> if the Cloudera driver has been registered
     */
    public static boolean clouderaDriverAvailable() {
        return DatabaseUtility.getJDBCDriverClasses().contains(CLOUDERA_DRIVER_NAME);
    }
}
