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

import java.util.ArrayList;
import java.util.Map;

import org.eclipse.core.runtime.Platform;
import org.osgi.framework.Bundle;
import org.osgi.framework.Version;

import com.knime.bigdata.spark.core.version.DefaultSparkProvider;
import com.knime.bigdata.spark.core.version.FixedVersionCompatibilityChecker;
import com.knime.bigdata.spark.core.version.SparkVersion;

/**
 * Default driver bundle provider.
 *
 * @author Sascha Wolke, KNIME.com
 */
public class DefaultBundleGroupSparkJarProvider extends DefaultSparkProvider<BundleGroupSparkJarProvider> {

    /**
     * Creates a new driver bundle.
     * @param version - Spark version
     * @param drivers - Map with driver name string and bundles
     */
    public DefaultBundleGroupSparkJarProvider(final SparkVersion version, final Map<String, Bundle[]> drivers) {
        super(new FixedVersionCompatibilityChecker(version), getProviders(version, drivers));
    }

    /** Converts driver name and bundles into {@link BundleGroupSparkJarProvider}s */
    private static BundleGroupSparkJarProvider[] getProviders(final SparkVersion version, final Map<String, Bundle[]> drivers) {
        final ArrayList<BundleGroupSparkJarProvider> provider = new ArrayList<>(drivers.size());
        for (Map.Entry<String, Bundle[]> driver : drivers.entrySet()) {
            provider.add(new BundleGroupSparkJarProvider(version, driver.getKey(), driver.getValue()));
        }
        return provider.toArray(new BundleGroupSparkJarProvider[0]);
    }

    /**
     * Shorthand method to find bundles by version.
     * @param name - Symbolic bundle name
     * @param inMinVersion - Inclusive minimal version (match bundles greater or equal this version)
     * @return Bundle matching version constraints
     */
    public static Bundle getBundle(final String name, final String inMinVersion) {
        final Bundle bundles[] = Platform.getBundles(name, inMinVersion);

        if (bundles != null && bundles.length > 0) {
            return bundles[0];
        } else {
            throw new RuntimeException("Unable to findle bundle " + name + " with min version " + inMinVersion);
        }
    }

    /**
     * Shorthand method to find bundles by version.
     * @param name - Symbolic bundle name
     * @param inMinVersion - Inclusive minimal version (match bundles greater or equal this version)
     * @param exMaxVersion - Exclusive maximal version (match bundles lower this version)
     * @return Bundle matching version constraints
     */
    public static Bundle getBundle(final String name, final String inMinVersion, final String exMaxVersion) {
        final Bundle bundles[] = Platform.getBundles(name, inMinVersion);
        final Version max = Version.parseVersion(exMaxVersion);

        for (int i = 0; i < bundles.length; i++) {
            if (bundles[i].getVersion().compareTo(max) < 0) {
                return bundles[i];
            }
        }

        throw new RuntimeException("Unable to findle bundle " + name + " with min version " + inMinVersion + " and max version " + exMaxVersion);
    }
}
