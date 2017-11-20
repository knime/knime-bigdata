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
package org.knime.bigdata.hive;

import java.io.File;
import java.net.URL;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.eclipse.core.runtime.FileLocator;
import org.eclipse.core.runtime.Path;
import org.eclipse.ui.plugin.AbstractUIPlugin;
import org.knime.bigdata.commons.config.CommonConfigContainer;
import org.osgi.framework.BundleContext;


/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class HivePlugin extends AbstractUIPlugin {
 // The shared instance.
    private static HivePlugin plugin;

    private String m_pluginRootPath;

    /**
     * The constructor.
     */
    public HivePlugin() {
        plugin = this;
        CommonConfigContainer.getInstance().hiveSupported();
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

        // don't set this to DEBUG (other than for testing reasons), because the Hive driver will log
        // the received data with DEBUG priority, resulting tons of logspam.
        Logger.getLogger("org.apache.hive").setLevel(Level.INFO);

        // we are setting zookeeper to debug because it allows to debug zookeeper service discovery a little better
        // (and it doesn't produce a lot of logspam).
        Logger.getLogger("org.apache.zookeeper").setLevel(Level.DEBUG);
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
    }

    /**
     * Returns the shared instance.
     *
     * @return The shared instance
     */
    public static HivePlugin getDefault() {
        return plugin;
    }

    /**
     * @return the absolute root path of this plugin
     */
    public String getPluginRootPath() {
        return m_pluginRootPath;
    }
}
