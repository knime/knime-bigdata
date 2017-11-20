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
 *   Created on Sep 22, 2017 by bjoern
 */
package org.knime.bigdata.spark.node;

import org.eclipse.core.runtime.Plugin;
import org.knime.bigdata.spark.node.knosp.KNOSPHelper;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;

/**
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class SparkNodePlugin extends Plugin {

    // The shared instance
    private static SparkNodePlugin plugin;

    private static KNOSPHelper knospHelper = null;

    /**
     * The constructor
     */
    public SparkNodePlugin() {
        synchronized (getClass()) {
            plugin = this;
        }
    }

    @Override
    public void stop(final BundleContext context) throws Exception {
        synchronized (getClass()) {
            plugin = null;
        }
        super.stop(context);
    }

    /**
     * Returns the shared instance.
     *
     * @return the shared instance.
     */
    public synchronized static SparkNodePlugin getDefault() {
        return plugin;
    }

    /**
     * Retrieves the singleton instance of {@link KNOSPHelper} from the OSGI service registry and returns it.
     *
     * @return a singleton instance of {@link KNOSPHelper}.
     * @throws IllegalStateException if the KNIME-on-Spark plugins that provide the instance of {@link KNOSPHelper} are
     *             not available, or if something else went wrong while trying to obtain the instance.
     */
    public synchronized static KNOSPHelper getKNOSPHelper() {
        if (knospHelper == null) {
            final Bundle bundle = plugin.getBundle();
            final BundleContext bundleContext = bundle.getBundleContext();

            final ServiceReference<KNOSPHelper> serviceRef =  bundleContext.getServiceReference(KNOSPHelper.class);
            if (serviceRef == null) {
                throw new IllegalStateException("KNIME-on-Spark extension is missing. Please add it to the KNIME installation.");
            }

            knospHelper = bundleContext.getService(serviceRef);
            if (knospHelper == null) {
                throw new IllegalStateException("KNIME-on-Spark extension is missing. Please add it to the KNIME installation.");
            }
        }
        return knospHelper;
    }
}
