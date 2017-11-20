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
 *   Created on 29.05.2015 by koetter
 */
package org.knime.bigdata.spark.core;

import java.io.File;
import java.net.URL;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.ext.RuntimeDelegate;

import org.eclipse.core.runtime.FileLocator;
import org.eclipse.core.runtime.Path;
import org.eclipse.ui.plugin.AbstractUIPlugin;
import org.osgi.framework.BundleContext;

import org.knime.bigdata.commons.config.CommonConfigContainer;
import com.knime.licenses.LicenseChecker;
import com.knime.licenses.LicenseFeatures;
import com.knime.licenses.LicenseUtil;


/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkPlugin extends AbstractUIPlugin {

    /**
     * Holds the singleton instance of the Spark plugin, once it has been created by the OSGI framework.
     */
    private static SparkPlugin plugin;

    private String m_pluginRootPath;

    private final ExecutorService m_executor = Executors.newSingleThreadExecutor();

    /**
     * {@link LicenseChecker} to use.
     */
    public static final LicenseChecker LICENSE_CHECKER = new LicenseUtil(LicenseFeatures.SparkExecutor);

    /**
     * The constructor.
     */
    public SparkPlugin() {
        plugin = this;
        CommonConfigContainer.getInstance().sparkSupported();
    }

    /**
     * This method is called upon plug-in activation.
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
     * @param context The bundle context.
     * @throws Exception If cause by super class.
     */
    @Override
    public void stop(final BundleContext context) throws Exception {
        plugin = null;
        super.stop(context);
        m_executor.shutdown();
        m_executor.awaitTermination(5, TimeUnit.SECONDS);
    }

    private void initializeJaxRSRuntime() {
        // The JAX-RS interface is in a different plug-in than the CXF implementation. Therefore the interface classes
        // won't find the implementation via the default ContextFinder classloader. We set the current classes's
        // classloader as context classloader and then it will find the service definition from this plug-in.
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            // silence the missing Aries Blueprint warnings
            Logger.getLogger("org.apache.cxf.bus.blueprint.NamespaceHandlerRegisterer").setLevel(Level.SEVERE);
            RuntimeDelegate.getInstance();
        } finally {
            Thread.currentThread().setContextClassLoader(cl);
        }
    }


    /**
     * @param r {@link Runnable} to execute with the Spark thread pool
     */
    public void addJob(final Runnable r) {
        m_executor.submit(r);
    }

    /**
     * Returns the singleton instance of the Spark plugin, once it has been created by the OSGI framework.
     *
     * @return The singleton instance of the Spark plugin
     */
    public static SparkPlugin getDefault() {
        return plugin;
    }

    /**
     * @return the absolute root path of this plugin
     */
    public String getPluginRootPath() {
        return m_pluginRootPath;
    }
}
