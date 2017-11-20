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
 *   Created on 13.02.2016 by koetter
 */
package com.knime.bigdata.spark.core.jar;

import com.knime.bigdata.spark.core.version.SparkProvider;

/**
 * This class has to be implemented by all plugins that provide Java classes that need to be present in a (remote)
 * remote Spark context. Typical implementors are plugins that provide Spark jobs.
 *
 * @author Tobias Koetter, KNIME.com
 */
public interface SparkJarProvider extends SparkProvider {

    /**
     * Invoked to collect all classes that should be present in the (remote) Spark context.
     *
     * @param collector the {@link JarCollector} that collects all java classes that should be send to the Spark cluster
     */
    void collect(final JarCollector collector);

    /**
     * Invoked to build the job jar descriptor, that lists all the providers. A provider is usually an
     * OSGI bundle. The provider ID must also contain version information. An example provider ID
     * would be: com.knime.bigdata.spark.core_1.6.0.v201607081132
     *
     * @return the provider ID as a String.
     */
    String getProviderID();
}
