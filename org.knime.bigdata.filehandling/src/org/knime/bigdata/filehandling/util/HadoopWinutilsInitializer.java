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
 *   Created on Jun 27, 2018 by bjoern
 */
package org.knime.bigdata.filehandling.util;

import java.io.File;
import java.io.IOException;

import org.knime.bigdata.hdfs.FileHandlingPlugin;
import org.knime.core.node.NodeLogger;

/**
 * Utility class to initialize the hadoop.home.dir system property, which is used by the Hadoop library for local file
 * system access on Windows. This is a band-aid for AP-9585 until we upgrade to Parquet 1.10 which is expected to work
 * without the Hadoop library when only writing to the local file system.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class HadoopWinutilsInitializer {

    private static final NodeLogger LOG = NodeLogger.getLogger(HadoopWinutilsInitializer.class);

    private static final String HADOOP_HOME_SYSPROPERTY = "hadoop.home.dir";

    /**
     * Initializes the hadoop.home.dir system property so that the Hadoop library can find winutils.exe, which is
     * required for local file system access via the HDFS API on Windows.
     *
     * @throws IOException
     */
    public synchronized static void ensureInitialized() throws IOException {
        final String hadoopHome =
            new File(FileHandlingPlugin.getDefault().getPluginRootPath(), "hadoop_home").getCanonicalPath();
        if (System.getProperty(HADOOP_HOME_SYSPROPERTY) == null) {
            LOG.info(String.format("Setting system property %s to %s (for Hadoop winutils).", HADOOP_HOME_SYSPROPERTY,
                hadoopHome));
            System.setProperty(HADOOP_HOME_SYSPROPERTY, hadoopHome);
        } else {
            LOG.warn(String.format("System property %s is already set to %s. Doing nothing (not overwriting).",
                HADOOP_HOME_SYSPROPERTY, System.getProperty(HADOOP_HOME_SYSPROPERTY)));
        }
    }

}
