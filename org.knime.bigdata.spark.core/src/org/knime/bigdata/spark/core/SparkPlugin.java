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

import org.eclipse.core.runtime.FileLocator;
import org.eclipse.core.runtime.Path;
import org.eclipse.ui.plugin.AbstractUIPlugin;
import org.knime.bigdata.commons.config.EclipsePreferencesHelper;
import org.knime.bigdata.spark.core.util.BackgroundTasks;
import org.osgi.framework.BundleContext;


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

    /**
     * The constructor.
     */
    public SparkPlugin() {
        plugin = this;
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
        EclipsePreferencesHelper.checkForLegacyPreferences(getBundle().getSymbolicName());
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
        BackgroundTasks.shutdown();
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
