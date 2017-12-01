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
 *   Created on Jun 10, 2016 by bjoern
 */
package org.knime.bigdata.spark.core.jar;

import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.knime.bigdata.spark.core.job.SparkClass;

/**
 * Common predicates to determine, which folders and jars of KNIME Extension for Apache Spark plugins should be scanned
 * for classes annotated with {@link SparkClass}. Use these in conjunction with the
 * {@link DefaultSparkJarProvider#DefaultSparkJarProvider(org.knime.bigdata.spark.core.version.CompatibilityChecker, Predicate...)}
 * constructor.
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public interface KNIMEPluginScanPredicates {

    /**Class name matcher for classes within the Spark core package.*/
    public static final Predicate<String> KNIME_CORE_PLUGIN_PREDICATE =
        Pattern.compile("org\\.knime\\.bigdata\\.spark\\.core").asPredicate();
    /**Class name matcher for classes within the Spark node package.*/
    public static final Predicate<String> KNIME_NODE_PLUGIN_PREDICATE =
        Pattern.compile("org\\.knime\\.bigdata\\.spark\\.node").asPredicate();
    /**Class name matcher for classes within the Spark job packages.*/
    public static final Predicate<String> KNIME_JOBS_PLUGIN_PREDICATE =
        Pattern.compile("org\\.knime\\.bigdata\\.spark[0-9_]*+").asPredicate();

    /**The knime.jar predicate that is used to identify the knime.jar to search for Spark classes.*/
    public static final Predicate<String> KNIME_JAR_PREDICATE = Pattern.compile("knime\\.jar").asPredicate();

    /**Class name matcher for the jobserver utils jar.*/
    public static final Predicate<String> KNIME_JOBSERVER_UTILS_JAR_PREDICATE =
        Pattern.compile("knime-jobserver-utils-.+\\.jar").asPredicate();
}
