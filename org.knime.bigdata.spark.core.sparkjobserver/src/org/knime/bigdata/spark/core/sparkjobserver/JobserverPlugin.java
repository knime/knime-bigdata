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
package org.knime.bigdata.spark.core.sparkjobserver;

import java.io.File;
import java.net.URL;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.ext.RuntimeDelegate;

import org.eclipse.core.runtime.FileLocator;
import org.eclipse.core.runtime.Path;
import org.eclipse.ui.plugin.AbstractUIPlugin;
import org.knime.bigdata.spark.core.version.CompatibilityChecker;
import org.knime.bigdata.spark.core.version.FixedVersionCompatibilityChecker;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.osgi.framework.BundleContext;

/**
 * OSGI Bundle Activator for the Spark Jobserver connector plugin.
 * 
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class JobserverPlugin extends AbstractUIPlugin {

	/**
	 * Compatibility checker for the Spark versions lower than 3.0, supported by the Jobserver connector plugin.
	 */
	public static final CompatibilityChecker JOBSERVER_SPARK_VERSION_CHECKER = new FixedVersionCompatibilityChecker(
			SparkVersion.V_1_2, SparkVersion.V_1_3, SparkVersion.V_1_5, SparkVersion.V_1_6, SparkVersion.V_1_6_CDH_5_9,
			SparkVersion.V_2_0, SparkVersion.V_2_1, SparkVersion.V_2_2, SparkVersion.V_2_3, SparkVersion.V_2_4);

	/**
	 * The highest Spark 2.x version supported by the Jobserver integration.
	 */
	public static final SparkVersion HIGHEST_SUPPORTED_SPARK_VERSION = SparkVersion.V_2_4;

	/**
	 * Holds the singleton instance of the Spark Jobserver connector plugin,
	 * once it has been created by the OSGI framework.
	 */
	private static volatile JobserverPlugin plugin;

	private String m_pluginRootPath;

	/**
	 * The constructor.
	 */
	public JobserverPlugin() {
		synchronized (getClass()) {
			plugin = this;
		}
	}

	/**
	 * This method is called upon plug-in activation.
	 * 
	 * @param context
	 *            The bundle context.
	 * @throws Exception
	 *             If cause by super class.
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
	 * @param context
	 *            The bundle context.
	 * @throws Exception
	 *             If cause by super class.
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
			// silence the missing Aries Blueprint warnings
			Logger.getLogger("org.apache.cxf.bus.blueprint.NamespaceHandlerRegisterer").setLevel(Level.SEVERE);
			RuntimeDelegate.getInstance();
		} finally {
			Thread.currentThread().setContextClassLoader(cl);
		}
	}

	/**
	 * Returns the singleton instance of this plugin, once it has
	 * been created by the OSGI framework.
	 *
	 * @return The singleton instance of this plugin.
	 */
	public static JobserverPlugin getDefault() {
		return plugin;
	}

	/**
	 * @return the absolute root path of this plugin
	 */
	public String getPluginRootPath() {
		return m_pluginRootPath;
	}
}
