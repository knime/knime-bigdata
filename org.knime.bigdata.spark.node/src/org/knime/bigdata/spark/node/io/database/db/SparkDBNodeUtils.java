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
 *   Created on Jun 11, 2019 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.spark.node.io.database.db;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.knime.bigdata.database.hive.Hive;
import org.knime.bigdata.database.impala.Impala;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.database.DBType;
import org.knime.database.connection.DBConnectionController;
import org.knime.database.connection.UrlDBConnectionController;
import org.knime.database.session.DBSession;
import org.knime.database.session.DBSessionInformation;

/**
 * Utility methods used in Spark to DB and DB to Spark nodes.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class SparkDBNodeUtils {

    private SparkDBNodeUtils() {
        // no instances
    }

    /**
     * Checks whether the input Database is compatible.
     * @param session the {@link DBSession} from the input port
     * @param toSpark <code>true</code> if this node goes to spark, <code>false</code> otherwise
     * @throws InvalidSettingsException If the wrong database is connected
     */
    public static void checkDBIdentifier(final DBSession session, final boolean toSpark) throws InvalidSettingsException {
        final DBType dbType = session.getDBType();
        if(dbType.equals(Hive.DB_TYPE) || dbType.equals(Impala.DB_TYPE)) {
            throw new InvalidSettingsException(String.format(
                "Unsupported connection, use %s node instead.", toSpark ? "Hive/Impala to Spark" : "Spark to Hive/Impala"));
        }
    }

    /**
     * Validate that given DB session support JDBC URLs.
     * @param sessionInfo the {@link DBSessionInformation} of the input port
     * @throws InvalidSettingsException if DB session does not support JDBC URLs
     */
    public static void checkJdbcUrlSupport(final DBSessionInformation sessionInfo) throws InvalidSettingsException {
        final DBConnectionController controller = sessionInfo.getConnectionController();
        if(controller instanceof UrlDBConnectionController) {
            final String jdbcUrl = ((UrlDBConnectionController) controller).getJdbcUrl();
            if (StringUtils.isBlank(jdbcUrl) || !jdbcUrl.startsWith("jdbc:")) {
                throw new InvalidSettingsException("No JDBC URL provided.");
            }
        } else {
            throw new InvalidSettingsException("DB to Spark only works with URL based database connections");
        }
    }

    /**
     * Collect driver jar files for spark job upload.
     *
     * @param session the {@link DBSession} of the input port
     * @return {@link File} reference of driver files
     */
    public static List<File> getDriverFiles(final DBSession session) {
        final ArrayList<File> jarFiles = new ArrayList<>();
        for(Path p : session.getDriver().getDriverFiles()) {
            jarFiles.add(p.toFile());
        }
        return jarFiles;
    }

    /**
     * Driver class name to use.
     *
     * @param session the {@link DBSession} of the input port
     * @return Class name of the driver to use
     */
    public static String getDriverClass(final DBSession session) {
        return session.getDriver().getDriverDefinition().getDriverClass();
    }

    /**
     * Try to detect missing JDBC driver error and throw an exception with hints how to fix the problem.
     * @param logger {@link NodeLogger} to use for debug messages
     * @param e Exception to test for missing JDBC drivers
     * @throws InvalidSettingsException if missing JDBC drivers exception was detected
     */
    public static void detectMissingDriverException(final NodeLogger logger, final KNIMESparkException e)
        throws InvalidSettingsException {

        final String message = e.getMessage();
        if (message != null && message.contains("Failed to load JDBC data: No suitable driver")) {
            logger.debug("Required JDBC driver not found in cluster. Original error message: " + e.getMessage());
            throw new InvalidSettingsException("Required JDBC driver not found. Enable the 'Upload local JDBC driver' "
                + "option in the node dialog to upload the required driver files to the cluster.");
        }
    }
}
