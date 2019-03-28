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
package org.knime.bigdata.hdfs;

import java.io.File;
import java.net.URL;

import javax.ws.rs.ext.RuntimeDelegate;

import org.eclipse.core.runtime.FileLocator;
import org.eclipse.core.runtime.Path;
import org.eclipse.ui.plugin.AbstractUIPlugin;
import org.knime.bigdata.commons.config.CommonConfigContainer;
import org.knime.bigdata.commons.hadoop.UserGroupUtil;
import org.knime.bigdata.filehandling.util.HadoopWinutilsInitializer;
import org.knime.core.node.NodeLogger;
import org.osgi.framework.BundleContext;

import com.sun.ws.rs.ext.RuntimeDelegateImpl;

/**
 * Plugin activator for the big data filehandling plugin.
 *
 * @author Tobias Koetter, KNIME GmbH
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class FileHandlingPlugin extends AbstractUIPlugin {

    private static final NodeLogger LOG = NodeLogger.getLogger(FileHandlingPlugin.class);

    // The shared instance.
    private static FileHandlingPlugin plugin;

    private String m_pluginRootPath;

    /**
     * The constructor.
     */
    public FileHandlingPlugin() {
        plugin = this;
        CommonConfigContainer.getInstance().hdfsSupported();
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

        try {
            // This method initializes the JAX-RS 1.x RuntimeDelegate, which is provided by com.sun.jersey.core.
            // Due to the way the RuntimeDelegates is used deep down in some libraries, we may not get a good error message
            // if this doesn't work for some reason (which sometimes happens on broken installations). This code here
            // at least fails with a usable error message.
            if (RuntimeDelegate.getInstance() == null) {
                throw new RuntimeException("No implementation found");
            }
        } catch (Throwable t) {
            LOG.error("Failed to initialize the JAX-RS 1.x RuntimeDelegate. Provided error message: " + t.getMessage(),
                t);
            RuntimeDelegate.setInstance(new RuntimeDelegateImpl());
        }

        HadoopWinutilsInitializer.ensureInitialized();
        UserGroupUtil.initHadoopConfigurationAndUGI(false);
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

    /**
     * Returns the shared instance.
     *
     * @return The shared instance
     */
    public static FileHandlingPlugin getDefault() {
        return plugin;
    }

    /**
     * @return the absolute root path of this plugin
     */
    public String getPluginRootPath() {
        return m_pluginRootPath;
    }
}
