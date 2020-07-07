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

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.eclipse.core.runtime.FileLocator;
import org.eclipse.core.runtime.Platform;
import org.knime.core.node.NodeLogger;
import org.osgi.framework.Bundle;
import org.osgi.framework.FrameworkUtil;

/**
 * Utility class to initialize the Hadoop libraries.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class HadoopInitializer {

    private static final NodeLogger LOG = NodeLogger.getLogger(HadoopInitializer.class);

    private static final String HADOOP_HOME_SYSPROPERTY = "hadoop.home.dir";

    private static final String[] HADOOP_HOME_FILES = new String[] {
        "winutils.exe", "hadoop.dll", "msvcr100.dll", "LICENSE.txt"
    };

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

        // Ensure there is an Hadoop home set on windows, this is not required on Linux/MacOS
        if (Platform.getOS().equals(Platform.OS_WIN32)) {
            if (System.getProperty(HADOOP_HOME_SYSPROPERTY) == null) {
                final String hadoopHome = createHadoopHome();
                LOG.debug(String.format("Setting system property %s to %s (for Hadoop winutils).",
                    HADOOP_HOME_SYSPROPERTY, hadoopHome));
                System.setProperty(HADOOP_HOME_SYSPROPERTY, hadoopHome);
            } else {
                LOG.debug(String.format("System property %s is already set to %s. Doing nothing (not overwriting).",
                    HADOOP_HOME_SYSPROPERTY, System.getProperty(HADOOP_HOME_SYSPROPERTY)));
            }
        }

        UserGroupUtil.initHadoopConfigurationAndUGI(false);

        isInitialized = true;
    }

    /**
     * Create a temporary Hadoop home directory and copy all {@link #HADOOP_HOME_FILES} into a sub directory called
     * {@code bin}. The source files can be exists in this plugin or a fragment.
     *
     * @return path to temporary Hadoop home directory
     * @throws IOException
     */
    private static String createHadoopHome() throws IOException {
        final Path tmpHadoopDir = Files.createTempDirectory("hadoop_home_");
        final Path tmpHadoopBinDir = Files.createDirectories(tmpHadoopDir.resolve("bin"));
        final Bundle bundle = FrameworkUtil.getBundle(HadoopInitializer.class);

        for (String filename : HADOOP_HOME_FILES) {
            final URL localUrl = FileLocator
                .resolve(FileLocator.find(bundle, new org.eclipse.core.runtime.Path("hadoop_home/bin/" + filename)));

            if (localUrl != null) {
                try {
                    Files.copy(Paths.get(localUrl.toURI()), tmpHadoopBinDir.resolve(filename));
                } catch (URISyntaxException e) {
                    throw new IOException(e);
                }
            } else {
                LOG.warn(String.format("Unable to find '%s' at hadoop home creation.", filename));
            }
        }

        return tmpHadoopDir.toFile().getCanonicalPath();
    }

}
