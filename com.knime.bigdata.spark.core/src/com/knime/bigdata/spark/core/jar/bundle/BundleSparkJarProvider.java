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
 */
package com.knime.bigdata.spark.core.jar.bundle;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.function.Predicate;
import java.util.jar.JarEntry;

import org.eclipse.core.runtime.FileLocator;
import org.osgi.framework.Bundle;

import com.knime.bigdata.spark.core.jar.JarCollector;
import com.knime.bigdata.spark.core.jar.JarPacker;
import com.knime.bigdata.spark.core.jar.SparkJarProvider;
import com.knime.bigdata.spark.core.version.CompatibilityChecker;
import com.knime.bigdata.spark.core.version.SparkVersion;

/**
 * Provides a OSGi bundle as Spark jar Provider.
 *
 * @author Sascha Wolke, KNIME.com
 */
public class BundleSparkJarProvider implements SparkJarProvider {
    private final Set<Predicate<JarEntry>> META_FILTER = Collections.singleton(JarPacker.META_INF_FILTER);

    private final CompatibilityChecker m_checker;

    private final Bundle m_bundle;

    /**
     * All files (except the META-INF directory) of given bundle will be added to jar.
     *
     * @param checker - Compatibility checker
     * @param bundle - Bundle to add
     */
    public BundleSparkJarProvider(final CompatibilityChecker checker, final Bundle bundle) {
        m_checker = checker;
        m_bundle = bundle;
    }

    @Override
    public CompatibilityChecker getChecker() {
        return m_checker;
    }

    @Override
    public boolean supportSpark(final SparkVersion sparkVersion) {
        return m_checker.supportSpark(sparkVersion);
    }

    @Override
    public Set<SparkVersion> getSupportedSparkVersions() {
        return m_checker.getSupportedSparkVersions();
    }

    @Override
    public void collect(final JarCollector collector) {
        try {
            final File bundleFile = FileLocator.getBundleFile(m_bundle);

            if (bundleFile.isDirectory()) {
                final File binDir = new File(bundleFile, "bin");
                if (binDir.isDirectory()) {
                    collector.addDirectory(binDir);
                } else {
                    collector.addDirectory(bundleFile);
                }
            } else {
                collector.addJar(bundleFile, META_FILTER);
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to add bundle file to job jar: " + e.getMessage(), e);
        }
    }

    @Override
    public String getProviderID() {
        return String.format("%s:%s", m_bundle.getSymbolicName(), m_bundle.getVersion().toString());
    }
}
