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
 */
package org.knime.bigdata.databricks;

import java.io.File;
import java.net.URL;

import javax.ws.rs.ext.RuntimeDelegate;

import org.eclipse.core.runtime.FileLocator;
import org.eclipse.core.runtime.Path;
import org.eclipse.ui.plugin.AbstractUIPlugin;
import org.osgi.framework.BundleContext;

/**
 * OSGI Bundle Activator for the Databricks connector plugin.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class DatabricksPlugin extends AbstractUIPlugin {

    /**
     * Holds the singleton instance of the Databricks connector plugin, once it has been created by the OSGI framework.
     */
    private static volatile DatabricksPlugin plugin;

    private String m_pluginRootPath;

    /**
     * The constructor.
     */
    public DatabricksPlugin() {
        synchronized (DatabricksPlugin.class) {
            plugin = this;
        }
    }

    /**
     * This method is called upon plug-in activation.
     *
     * @param context The bundle context.
     * @throws Exception If cause by super class.
     */
    @Override
    public void start(final BundleContext context) throws Exception {
        super.start(context);
        final URL pluginURL = FileLocator.resolve(FileLocator.find(plugin.getBundle(), new Path(""), null));
        final File tmpFile = new File(pluginURL.getPath());
        m_pluginRootPath = tmpFile.getAbsolutePath();
        initializeJaxRSRuntime();
    }

    /**
     * This method is called when the plug-in is stopped.
     *
     * @param context The bundle context.
     * @throws Exception If cause by super class.
     */
    @Override
    public void stop(final BundleContext context) throws Exception {
        plugin = null;
        super.stop(context);
    }

    private void initializeJaxRSRuntime() {
        // The JAX-RS interface is in a different plug-in than the CXF
        // implementation. Therefore the interface classes
        // won't find the implementation via the default ContextFinder
        // classloader. We set the current classes's
        // classloader as context classloader and then it will find the service
        // definition from this plug-in.
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            RuntimeDelegate.getInstance();
        } finally {
            Thread.currentThread().setContextClassLoader(cl);
        }
    }

    /**
     * Returns the singleton instance of this plugin, once it has been created by the OSGI framework.
     *
     * @return The singleton instance of this plugin.
     */
    public static DatabricksPlugin getDefault() {
        return plugin;
    }

    /**
     * @return the absolute root path of this plugin
     */
    public String getPluginRootPath() {
        return m_pluginRootPath;
    }
}
