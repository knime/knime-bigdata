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
 *   Created on Nov 1, 2016 by Sascha Wolke, KNIME.com
 */
package com.knime.bigdata.spark.core.jar.bundle;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.knime.bigdata.spark.core.version.SparkProviderRegistry;
import com.knime.bigdata.spark.core.version.SparkProviderWithElements;
import com.knime.bigdata.spark.core.version.SparkVersion;

/**
 * A jar registry based on OSGi bundles.
 *
 * @author Sascha Wolke, KNIME.com
 */
public class BundleGroupSparkJarRegistry extends SparkProviderRegistry<SparkProviderWithElements<BundleGroupSparkJarProvider>> {

    /** The id of the converter extension point. */
    public static final String EXT_POINT_ID = "com.knime.bigdata.spark.core.BundleGroupSparkJarProvider";

    private static BundleGroupSparkJarRegistry instance;

    private final Map<SparkVersion, Map<String, BundleGroupSparkJarProvider>> m_drivers = new LinkedHashMap<>();

    private BundleGroupSparkJarRegistry() {}

    /**
     * Returns the only instance of this class.
     * @return the only instance
     */
    public synchronized static BundleGroupSparkJarRegistry getInstance() {
        if (instance == null) {
            instance = new BundleGroupSparkJarRegistry();
            instance.registerExtensions(EXT_POINT_ID);
        }
        return instance;
    }

    @Override
    protected void addProvider(final SparkProviderWithElements<BundleGroupSparkJarProvider> provider) {
        for(SparkVersion version : provider.getSupportedSparkVersions()) {
            Map<String, BundleGroupSparkJarProvider> versionProvider = m_drivers.get(version);

            if (versionProvider == null) {
                versionProvider = new HashMap<>();
                m_drivers.put(version, versionProvider);
            }

            for (BundleGroupSparkJarProvider driver : provider.get()) {
                versionProvider.put(driver.getName(), driver);
            }
        }
    }

    /**
     * Returns a list bundled jars for given format.
     * @param sparkVersion - Spark version
     * @param format - DataSources format
     * @return List contains bundled jar files or empty list.
     */
    public synchronized static List<File> getBundledDriverJars(final SparkVersion sparkVersion, final String format) {
        Map<String, BundleGroupSparkJarProvider> versionRegistry = getInstance().m_drivers.get(sparkVersion);

        if (versionRegistry != null && versionRegistry.containsKey(format)) {
            return Arrays.asList(versionRegistry.get(format).getJobJar().getJarFile());
        } else {
            return Collections.emptyList();
        }
    }

}
