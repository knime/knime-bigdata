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
 *   Created on Nov 11, 2020 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.spark.core.livy.context;

import java.time.ZoneId;
import java.util.Map;
import java.util.Optional;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.bigdata.spark.node.util.context.create.time.TimeSettings.TimeShiftStrategy;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication.AuthenticationType;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.connections.FSConnectionRegistry;

/**
 * Livy Spark context configuration using a NIO file system connection.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class LivySparkContextFileSystemConfig extends LivySparkContextConfig {

    private final String m_fileSystemId;

    /**
     * Default constructor.
     *
     * @param sparkVersion
     * @param livyUrl
     * @param authenticationType
     * @param stagingAreaFolder
     * @param connectTimeoutSeconds
     * @param responseTimeoutSeconds
     * @param jobCheckFrequencySeconds
     * @param customSparkSettings
     * @param sparkContextId
     * @param fileSystemId
     * @param timeShiftStrategy
     * @param timeShiftZoneId optional time shift zone ID, might by {@code null}
     * @param failOnDifferentClusterTimeZone {@code true} if context creation should fail on different cluster time zone
     */
    public LivySparkContextFileSystemConfig(final SparkVersion sparkVersion, final String livyUrl,
        final AuthenticationType authenticationType, final String stagingAreaFolder, final int connectTimeoutSeconds,
        final int responseTimeoutSeconds, final int jobCheckFrequencySeconds,
        final Map<String, String> customSparkSettings, final SparkContextID sparkContextId,
        final String fileSystemId,
        final TimeShiftStrategy timeShiftStrategy, final ZoneId timeShiftZoneId,
        final boolean failOnDifferentClusterTimeZone) {

        super(sparkVersion, livyUrl, authenticationType, stagingAreaFolder, connectTimeoutSeconds,
            responseTimeoutSeconds, jobCheckFrequencySeconds, customSparkSettings, sparkContextId, timeShiftStrategy,
            timeShiftZoneId, failOnDifferentClusterTimeZone);

        m_fileSystemId = fileSystemId;
    }

    @Override
    @SuppressWarnings("resource")
    public RemoteFSController createRemoteFSController() throws KNIMESparkException {
        final Optional<FSConnection> fsConnection = FSConnectionRegistry.getInstance().retrieve(m_fileSystemId);

        if (fsConnection.isPresent()) {
            return new RemoteFSControllerNIO(fsConnection.get(), getStagingAreaFolder());
        } else {
            throw new KNIMESparkException("File system connection unavailable. Restart connector node.");
        }
    }

    /**
     * Indicates whether some other object shares the same file system type and context configuration.
     *
     * @see LivySparkContextConfig#equals(Object)
     */
    @Override
    public boolean equals(final Object obj) { // NOSONAR super class implements hashCode
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        return super.equals(obj);
    }

}
