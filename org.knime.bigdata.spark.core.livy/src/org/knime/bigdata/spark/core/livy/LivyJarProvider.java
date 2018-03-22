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

import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.knime.bigdata.spark.core.jar.BaseSparkJarProvider;
import org.knime.bigdata.spark.core.jar.DefaultSparkJarProvider;
import org.knime.bigdata.spark.core.jar.KNIMEPluginScanPredicates;

/**
 * Jar provider implementation for Apache Livy connector.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class LivyJarProvider extends DefaultSparkJarProvider implements BaseSparkJarProvider {

	public static final Predicate<String> LIVY_PLUGIN_PREDICATE = Pattern
			.compile("org\\.knime\\.bigdata\\.spark\\.core\\.livy").asPredicate();
	
	/**
	 * Constructor.
	 */
	public LivyJarProvider() {
		super(LivyPlugin.LIVY_SPARK_VERSION_CHECKER,
				KNIMEPluginScanPredicates.KNIME_JAR_PREDICATE,
				LIVY_PLUGIN_PREDICATE);
	}
}
