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
package org.knime.bigdata.commons.hadoop;

import java.io.File;
import java.io.IOException;

import org.knime.bigdata.commons.CommonsPlugin;
import org.knime.core.node.NodeLogger;

/**
 * Utility class to initialize the Hadoop libraries.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class HadoopInitializer {

    private static final NodeLogger LOG = NodeLogger.getLogger(HadoopInitializer.class);

    private static final String HADOOP_HOME_SYSPROPERTY = "hadoop.home.dir";

    private static boolean isInitialized = false;

    /**
     * Initializes the Hadoop libraries, e.g. sets the hadoop.home.dir system property (for winutils.exe, which is
     * required for local file system access via the HDFS API on Windows) and sets an initial Hadoop configuration and
     * UGI.
     *
     * @throws IOException
     */
    public synchronized static void ensureInitialized() throws IOException {
        if (isInitialized) {
            return;
        }

        final String hadoopHome =
            new File(CommonsPlugin.getDefault().getPluginRootPath(), "hadoop_home").getCanonicalPath();
        if (System.getProperty(HADOOP_HOME_SYSPROPERTY) == null) {
            LOG.debug(String.format("Setting system property %s to %s (for Hadoop winutils).", HADOOP_HOME_SYSPROPERTY,
                hadoopHome));
            System.setProperty(HADOOP_HOME_SYSPROPERTY, hadoopHome);
        } else {
            LOG.debug(String.format("System property %s is already set to %s. Doing nothing (not overwriting).",
                HADOOP_HOME_SYSPROPERTY, System.getProperty(HADOOP_HOME_SYSPROPERTY)));
        }

        UserGroupUtil.initHadoopConfigurationAndUGI(false);

        isInitialized = true;
    }

}
