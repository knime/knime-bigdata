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
package org.knime.bigdata.spark.local;

import java.io.File;

import org.apache.log4j.Appender;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.eclipse.ui.plugin.AbstractUIPlugin;
import org.knime.core.node.KNIMEConstants;
import org.osgi.framework.BundleContext;

/**
 * Bundle activator for Spark local. This class sets up logging for local Spark.
 * 
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class SparkLocalPlugin extends AbstractUIPlugin {

	/**
	 * Singleton instance of the Spark local plugin.
	 */
	private static SparkLocalPlugin plugin;

	/**
	 * Log file for local Spark instances.
	 */
	public static final File SPARK_LOG_FILE = new File(KNIMEConstants.getKNIMEHomeDir(), "local_spark.log");

	/**
	 * Default constructor.
	 */
	public SparkLocalPlugin() {
		synchronized (getClass()) {
			plugin = this;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void start(final BundleContext context) throws Exception {
		super.start(context);
		final Layout sparkLogLayout = new PatternLayout("%d{dd--MM--yyyy HH:mm:ss,SSS} [%p] %30.30c - %m%n");
		final Appender sparkLogAppender = new RollingFileAppender(sparkLogLayout, SPARK_LOG_FILE.getAbsolutePath());

		configureLoggerForLocalSparkLog(sparkLogAppender, Logger.getLogger("org.apache.spark"));
		configureLoggerForLocalSparkLog(sparkLogAppender, Logger.getLogger("org.apache.hadoop.hive.metastore"));
		configureLoggerForLocalSparkLog(sparkLogAppender, Logger.getLogger("org.apache.hive.service"));
	}

	private static void configureLoggerForLocalSparkLog(final Appender sparkLogAppender, final Logger logger) {
		logger.setAdditivity(false);
		logger.removeAllAppenders();
		logger.addAppender(sparkLogAppender);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void stop(final BundleContext context) throws Exception {
		synchronized (getClass()) {
			plugin = null;
		}
		super.stop(context);
	}

	/**
	 * Returns the singleton instance.
	 *
	 * @return the singleton instance.
	 */
	public synchronized static SparkLocalPlugin getDefault() {
		return plugin;
	}
}
