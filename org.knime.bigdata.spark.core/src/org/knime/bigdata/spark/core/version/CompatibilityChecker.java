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
 *   Created on 29.01.2016 by koetter
 */
package org.knime.bigdata.spark.core.version;

import java.util.Set;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public interface CompatibilityChecker {

    /**
     * @param sparkVersion the Spark version to check e.g. 1.2, 1.3, etc
     * @return <code>true</code> if the version is supported
     */
    boolean supportSpark(SparkVersion sparkVersion);

    /**
     * @return a set with the {@link SparkVersion}s supported by this class.
     */
    Set<SparkVersion> getSupportedSparkVersions();
}
