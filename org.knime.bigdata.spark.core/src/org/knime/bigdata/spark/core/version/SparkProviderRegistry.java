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
 *   Created on 27.04.2016 by koetter
 */
package com.knime.bigdata.spark.core.version;

import org.eclipse.core.runtime.IConfigurationElement;
import org.eclipse.core.runtime.IExtensionPoint;
import org.eclipse.core.runtime.IExtensionRegistry;
import org.eclipse.core.runtime.Platform;
import org.knime.core.node.NodeLogger;

/**
 *
 * @author Tobias Koetter, KNIME.com
 * @param <P>
 */
public abstract class SparkProviderRegistry<P extends SparkProvider> {

    /**The attribute of the extension point.*/
    private static final String EXT_POINT_ATTR_DF = "ProviderClass";

    private static final NodeLogger LOGGER = NodeLogger.getLogger(SparkProviderRegistry.class);

    /**
     * Registers all extension point implementations.
     * @param extPointId
     */
    protected void registerExtensions(final String extPointId) {
        registerExtensions(extPointId, EXT_POINT_ATTR_DF);
    }

    /**
     * Registers all extension point implementations.
     * @param extPointId
     * @param extPointAttrDf
     */
    protected void registerExtensions(final String extPointId, final String extPointAttrDf) {
        try {
            final IExtensionRegistry registry = Platform.getExtensionRegistry();
            final IExtensionPoint point = registry.getExtensionPoint(extPointId);
            if (point == null) {
                LOGGER.error("Invalid extension point: " + extPointId);
                throw new IllegalStateException("ACTIVATION ERROR: --> Invalid extension point: " + extPointId);
            }
            for (final IConfigurationElement elem : point.getConfigurationElements()) {
                final String helperClass = elem.getAttribute(extPointAttrDf);
                final String decl = elem.getDeclaringExtension().getNamespaceIdentifier();
                if (helperClass == null || helperClass.isEmpty()) {
                    LOGGER.error("The extension '" + decl + "' doesn't provide the required attribute '"
                            + extPointAttrDf + "'");
                    LOGGER.error("Extension " + decl + " ignored.");
                    continue;
                }
                try {
                    LOGGER.debug("Registering Spark provider class: " + helperClass + " from extension: " + decl);
                    @SuppressWarnings("unchecked")
                    final P provider = (P)elem.createExecutableExtension(extPointAttrDf);
                    addProvider(provider);
                } catch (final Throwable t) {
                    LOGGER.error("Problems during initialization of Spark provider with id '" + helperClass
                        + "'. Exception: " + t.getMessage(), t);
                    if (decl != null) {
                        LOGGER.error("Extension " + decl + " ignored.", t);
                    }
                }
            }
        } catch (final Exception e) {
            LOGGER.error("Exception while registering Spark provider for extension point " + extPointId, e);
        }
    }

    /**
     * @param provider the {@link SparkProvider} to register
     */
    protected abstract void addProvider(final P provider);

}