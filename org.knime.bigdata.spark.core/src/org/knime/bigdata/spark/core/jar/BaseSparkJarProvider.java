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
 *   Created on Apr 28, 2016 by bjoern
 */
package org.knime.bigdata.spark.core.jar;

/**
 * Some plugins (e.g. org.knime.bigdata.spark.core and org.knime.bigdata.spark.node) are "base" jar providers, which
 * means they do not contain any Spark jobs, but provide classes that Spark jobs depend on. This interface is a marker
 * interface to indicate to {@link SparkJarRegistry} that a jar provider is a "base" jar provider. The difference
 * between "base" and "regular" jar providers, is that the "base" providers will not trigger Spark class collection by
 * themselves. To trigger Spark class collection, at least one "regular" jar provider has to exist.
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public interface BaseSparkJarProvider extends SparkJarProvider {

}
