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
 *   Created on 22.09.2015 by koetter
 */
package com.knime.bigdata.spark.util;

import org.eclipse.core.runtime.IConfigurationElement;
import org.eclipse.core.runtime.IExtensionPoint;
import org.eclipse.core.runtime.IExtensionRegistry;
import org.eclipse.core.runtime.Platform;
import org.knime.core.node.NodeLogger;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkJobRegistry {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(SparkJobRegistry.class);

    /**The id of the converter extension point.*/
    public static final String EXT_POINT_ID =
            "com.knime.bigdata.spark.jobs.api.SparkJobProvider";

    /**The attribute of the converter extension point.*/
    public static final String EXT_POINT_ATTR_DF = "SparkJobProvider";

    private static SparkJobRegistry instance = new SparkJobRegistry();

//    private JarCollector m_jobs;

    private SparkJobRegistry() {
        registerExtensionPoints();
    }

    /**
     * Returns the only instance of this class.
     * @return the only instance
     */
    public static SparkJobRegistry getInstance() {
        return instance;
    }

    /**
     * Registers all extension point implementations.
     */
    private void registerExtensionPoints() {
        try {
            final IExtensionRegistry registry = Platform.getExtensionRegistry();
            final IExtensionPoint point = registry.getExtensionPoint(EXT_POINT_ID);
            if (point == null) {
                LOGGER.error("Invalid extension point: " + EXT_POINT_ID);
                throw new IllegalStateException("ACTIVATION ERROR: --> Invalid extension point: " + EXT_POINT_ID);
            }
            for (final IConfigurationElement elem : point.getConfigurationElements()) {
                final String converter = elem.getAttribute(EXT_POINT_ATTR_DF);
                final String decl = elem.getDeclaringExtension().getUniqueIdentifier();

                if (converter == null || converter.isEmpty()) {
                    LOGGER.error("The extension '" + decl + "' doesn't provide the required attribute '"
                            + EXT_POINT_ATTR_DF + "'");
                    LOGGER.error("Extension " + decl + " ignored.");
                    continue;
                }
                try {
//                    final SparkJobProvider provider =
//                            (SparkJobProvider)elem.createExecutableExtension(EXT_POINT_ATTR_DF);
//                    addProvider(provider);
                } catch (final Throwable t) {
                    LOGGER.error("Problems during initialization of Spark TypeConverter (with id '" + converter
                        + "'.)", t);
                    if (decl != null) {
                        LOGGER.error("Extension " + decl + " ignored.", t);
                    }
                }
            }
        } catch (final Exception e) {
            LOGGER.error("Exception while registering aggregation operator extensions", e);
        }
    }
//
//    /**
//     * @param provider
//     */
//    private void addProvider(final SparkJobProvider provider) {
//        provider.getJar(m_jobs);
//    }
//
//    /**
//     * @return the path to the Spark jobs jar
//     */
//    public String getJobJarPath() {
//        return m_jobs.getJar().getAbsolutePath();
//    }
}
