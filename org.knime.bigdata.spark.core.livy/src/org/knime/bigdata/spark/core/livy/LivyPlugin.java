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
package org.knime.bigdata.spark.core.livy;

import java.io.File;
import java.net.URL;

import org.eclipse.core.runtime.FileLocator;
import org.eclipse.core.runtime.Path;
import org.eclipse.ui.plugin.AbstractUIPlugin;
import org.knime.bigdata.spark.core.version.CompatibilityChecker;
import org.knime.bigdata.spark.core.version.FixedVersionCompatibilityChecker;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.kerberos.api.KerberosProvider;
import org.osgi.framework.BundleContext;

/**
 * OSGI Bundle Activator for the Apache Livy connector plugin.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class LivyPlugin extends AbstractUIPlugin {

    /**
     * Compatibility checker for the Spark versions currently supported by the Livy connector plugin.
     */
    public static final CompatibilityChecker LIVY_SPARK_VERSION_CHECKER =
        new FixedVersionCompatibilityChecker(SparkVersion.V_2_2, SparkVersion.V_2_3, SparkVersion.V_2_4, SparkVersion.V_3_0);

    /**
     * Holds the singleton instance of the Apache Livy connector plugin, once it has been created by the OSGI framework.
     */
    private static volatile LivyPlugin plugin;

    private String m_pluginRootPath;

    /**
     * The constructor.
     */
    public LivyPlugin() {
        synchronized (LivyPlugin.class) {
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
        
        // ensure Kerberos debug logging is properly initialized
        KerberosProvider.ensureInitialized();
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
     * Returns the singleton instance of this plugin, once it has been created by the OSGI framework.
     *
     * @return The singleton instance of this plugin.
     */
    public static LivyPlugin getDefault() {
        return plugin;
    }

    /**
     * @return the absolute root path of this plugin
     */
    public String getPluginRootPath() {
        return m_pluginRootPath;
    }
}
